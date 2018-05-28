package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	yaml "gopkg.in/yaml.v2"
)

func main() {
	var port int
	var schemaPath string
	var seedsPath string

	flag.IntVar(&port, "port", 0, "port to listen on")
	flag.StringVar(&schemaPath, "schema", "", "path to schema file")
	flag.StringVar(&seedsPath, "seeds", "", "path to seeds file")
	flag.Parse()

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan)

	srv, err := bttest.NewServer(fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("error starting BigTable emulator: %v", err)
	}
	defer srv.Close()

	log.Printf("BigTable emulator listening at %s", srv.Addr)

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	loadSchema(ctx, srv, schemaPath)
	seedData(ctx, srv, seedsPath)

	<-stopChan
	log.Println("shutting down")
}

type Schema struct {
	Tables   []TableSchema `yaml:"tables"`
	Project  string        `yaml:"project"`
	Instance string        `yaml:"instance"`
}

type TableSchema struct {
	Name     string   `yaml:"name"`
	Families []string `yaml:"families"`
}

type SeedData map[TableName]SeedTable
type TableName string
type SeedTable map[RowKey]SeedRow
type RowKey string
type SeedRow map[CFName]SeedCF
type CFName string
type SeedCF map[ColumnName]string
type ColumnName string

func loadSchema(ctx context.Context, srv *bttest.Server, schemaPath string) {
	log.Println("loading schema")
	schema := Schema{}
	err := loadYamlFromFile(schemaPath, &schema)
	if err != nil {
		log.Panicf("error loading schema: %s", err)
	}

	adminClient, err := adminClient(ctx, srv)
	if err != nil {
		log.Panicf("error creating BigTable admin client: %s", err)
	}

	for _, table := range schema.Tables {
		log.Printf("creating table %q", table.Name)

		err := adminClient.CreateTable(ctx, table.Name)
		if err != nil {
			log.Fatalf("error creating table %q", err)
		}

		for _, family := range table.Families {
			log.Printf("creating family %q > %q", table.Name, family)
			adminClient.CreateColumnFamily(ctx, table.Name, family)
		}
	}

	if err != nil {
		log.Fatalf("error creating test table: %s", err)
	}
}

func seedData(ctx context.Context, srv *bttest.Server, seedsPath string) {
	log.Println("seeding data")
	seeds := make(SeedData)

	err := loadYamlFromFile(seedsPath, &seeds)
	if err != nil {
		log.Fatalf("error seeding data: %v", err)
	}

	client, err := newClient(ctx, srv)
	if err != nil {
		log.Fatalf("error connecting to BT: %v", err)
	}

	for tableName, rows := range seeds {
		log.Printf("seeding table %q", tableName)
		table := client.Open(string(tableName))

		for rowKey, row := range rows {
			mut := bigtable.NewMutation()

			for family, columns := range row {
				for column, data := range columns {
					mut.Set(string(family), string(column), bigtable.Now(), []byte(data))
				}
			}

			err = table.Apply(ctx, string(rowKey), mut)
			if err != nil {
				log.Fatalf("error applying mutation: %v", err)
			}
		}
	}
}

func loadYamlFromFile(path string, obj interface{}) error {
	rawYaml, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, "unable to open file")
	}

	return yaml.Unmarshal(rawYaml, obj)
}

func newClient(ctx context.Context, srv *bttest.Server) (*bigtable.Client, error) {
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	proj, instance := "proj", "instance"
	return bigtable.NewClient(ctx, proj, instance, option.WithGRPCConn(conn))
}

func adminClient(ctx context.Context, srv *bttest.Server) (*bigtable.AdminClient, error) {
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}

	proj, instance := "proj", "instance"
	return bigtable.NewAdminClient(ctx, proj, instance, option.WithGRPCConn(conn))
}
