package layer

import (
	"context"
	"encoding/csv"
	"github.com/google/uuid"
	cdl "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"os"
	"testing"
	"time"
)

func TestStartStopFileSystemDataLayer(t *testing.T) {
	configLocation := "./testconfig"
	serviceRunner := cdl.NewServiceRunner(NewOpenCypherDataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["path"] = "/tmp"
		return nil
	})
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func writeSampleCsv(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// create csv writer
	writer := csv.NewWriter(file)
	writer.Write([]string{"id", "name", "age", "worksfor"})
	writer.Write([]string{"1", "John", "30", "Mimiro"})
	writer.Write([]string{"2", "Jane", "25", "Mimiro"})
	writer.Write([]string{"3", "Jim", "35", "Mimiro"})
	writer.Flush()
	return writer.Error()
}

// test write full sync
func TestWriteFullSync(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	configLocation := "./testconfig"
	serviceRunner := cdl.NewServiceRunner(NewOpenCypherDataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["path"] = folderName
		return nil
	})

	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	// write entities
	batch := cdl.BatchInfo{SyncId: "1", IsLastBatch: true, IsStartBatch: true}
	writer, err := ds.FullSync(context.Background(), batch)
	if err != nil {
		t.Error(err)
	}

	entity := makeEntity("1")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	// create neo4j client
	driver, err := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "neo4j", ""))
	if err != nil {
		t.Error(err)
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	txn, err := session.BeginTransaction(ctx, func(config *neo4j.TransactionConfig) { config.Timeout = 15 * time.Minute })
	if err != nil {
		t.Error(err)
	}

	// write a neo4j query and execute to find the nodes created
	query := "MATCH (n:Person) WHERE n.source = 'people' OPTIONAL MATCH (n)-[r]->(m) RETURN n, COLLECT({rel: r, targetGid: m.gid}) AS relationships"

	result, err := txn.Run(ctx, query, nil)
	if err != nil {
		t.Error(err)
	}

	nodes, err := result.Collect(ctx)
	if err != nil {
		t.Error("Failed to cast result to []*neo4j.Node")
	}

	if len(nodes) == 0 {
		t.Error("Expected nodes to be created")
	}

	if len(nodes) != 1 {
		t.Error("Expected 1 node to be created")
	}

	// check node id
	node := nodes[0].Values[0].(dbtype.Node)
	if node.Props["gid"] != "http://data.sample.org/things/1" {
		t.Error("Expected node with gid http://data.sample.org/things/1")
	}

	// check name property
	if node.Props["name"] != "brian" {
		t.Error("Expected node with name brian")
	}

	// check the relationships
	relationships := nodes[0].Values[1].([]interface{})
	if len(relationships) != 1 {
		t.Error("Expected 1 relationship")
	}

	relationship := relationships[0].(map[string]interface{})
	if relationship["targetGid"] != "http://data.sample.org/things/mimiro" {
		t.Error("Expected relationship to target http://data.sample.org/things/mimiro")
	}

	err = txn.Commit(ctx)
	if err != nil {
		t.Error(err)
	}

	// modify entity and do another full sync
	entity.SetProperty("http://data.sample.org/name", "John Doe")
	entity.References = make(map[string]interface{})
	batch = cdl.BatchInfo{SyncId: "2", IsLastBatch: true, IsStartBatch: true}
	writer, err = ds.FullSync(context.Background(), batch)
	if err != nil {
		t.Error(err)
	}

	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// check state of the data
	txn, err = session.BeginTransaction(ctx, func(config *neo4j.TransactionConfig) { config.Timeout = 15 * time.Minute })
	if err != nil {
		t.Error(err)
	}

	// write a neo4j query and execute to find the nodes created
	query = "MATCH (n:Person) WHERE n.source = 'people' OPTIONAL MATCH (n)-[r]->(m) RETURN n, COLLECT({rel: r, targetGid: m.gid}) AS relationships"

	result, err = txn.Run(ctx, query, nil)
	if err != nil {
		t.Error(err)
	}

	nodes, err = result.Collect(ctx)
	if err != nil {
		t.Error("Failed to cast result to []*neo4j.Node")
	}

	if len(nodes) == 0 {
		t.Error("Expected nodes to exist")
	}

	if len(nodes) != 1 {
		t.Error("Expected 1 node to exist")
	}

	// check node id
	node = nodes[0].Values[0].(dbtype.Node)
	if node.Props["gid"] != "http://data.sample.org/things/1" {
		t.Error("Expected node with gid http://data.sample.org/things/1")
	}

	// check name property
	if node.Props["name"] != "John Doe" {
		t.Error("Expected node with John Doe")
	}

	// check the relationships
	relationships = nodes[0].Values[1].([]interface{})
	if len(relationships) > 1 {
		t.Error("Expected 0 relationships")
	}

	// get first rel and check targetGid is nil
	relationship = relationships[0].(map[string]interface{})
	if relationship["targetGid"] != nil {
		t.Error("Expected relationship to target nil")
	}

	err = txn.Commit(ctx)
	if err != nil {
		t.Error(err)
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}

}

func TestWriteIncremental(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	configLocation := "./testconfig"
	serviceRunner := cdl.NewServiceRunner(NewOpenCypherDataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["path"] = folderName
		return nil
	})

	// tidy up graph data before we start
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "neo4j", ""))

	if err != nil {
		t.Error(err)
	}

	defer driver.Close(ctx)
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err = session.Run(ctx, "MATCH (n:Person) WHERE n.source = 'people' DETACH DELETE n", nil)
	if err != nil {
		t.Error(err)
	}

	_, err = session.Run(ctx, "MATCH (n:Company) WHERE n.source = 'companies' DETACH DELETE n", nil)
	if err != nil {
		t.Error(err)
	}

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	// write entities
	writer, err := ds.Incremental(context.Background())
	if err != nil {
		t.Error(err)
	}

	entity := makeEntity("1")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	txn, err := session.BeginTransaction(ctx, func(config *neo4j.TransactionConfig) { config.Timeout = 15 * time.Minute })
	if err != nil {
		t.Error(err)
	}

	// write a neo4j query and execute to find the nodes created
	query := "MATCH (n:Person) WHERE n.source = 'people' OPTIONAL MATCH (n)-[r]->(m) RETURN n, COLLECT({rel: r, targetGid: m.gid}) AS relationships"

	result, err := txn.Run(ctx, query, nil)
	if err != nil {
		t.Error(err)
	}

	nodes, err := result.Collect(ctx)
	if err != nil {
		t.Error("Failed to cast result to []*neo4j.Node")
	}

	if len(nodes) == 0 {
		t.Error("Expected nodes to be created")
	}

	if len(nodes) != 1 {
		t.Error("Expected 1 node to be created")
	}

	// check node id
	node := nodes[0].Values[0].(dbtype.Node)
	if node.Props["gid"] != "http://data.sample.org/things/1" {
		t.Error("Expected node with gid http://data.sample.org/things/1")
	}

	// check name property
	if node.Props["name"] != "brian" {
		t.Error("Expected node with name brian")
	}

	// check the relationships
	relationships := nodes[0].Values[1].([]interface{})
	if len(relationships) != 1 {
		t.Error("Expected 1 relationship")
	}

	relationship := relationships[0].(map[string]interface{})
	if relationship["targetGid"] != "http://data.sample.org/things/mimiro" {
		t.Error("Expected relationship to target http://data.sample.org/things/mimiro")
	}

	err = txn.Commit(ctx)
	if err != nil {
		t.Error(err)
	}

	ds2, err := service.Dataset("companies")
	if err != nil {
		t.Error(err)
	}
	writer, err = ds2.Incremental(context.Background())
	if err != nil {
		t.Error(err)
	}

	entity = makeEntity("1")
	entity.SetID("http://data.sample.org/things/mimiro")
	entity.Properties = make(map[string]interface{})
	entity.SetProperty("http://data.sample.org/name", "Mimiro")
	entity.References = make(map[string]interface{})
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	session = driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	txn, err = session.BeginTransaction(ctx, func(config *neo4j.TransactionConfig) { config.Timeout = 15 * time.Minute })
	if err != nil {
		t.Error(err)
	}

	// write a neo4j query and execute to find the nodes created
	query = "MATCH (n:Company) WHERE n.source = 'companies' OPTIONAL MATCH (n)-[r]->(m) RETURN n, COLLECT({rel: r, targetGid: m.gid}) AS relationships"

	result, err = txn.Run(ctx, query, nil)
	if err != nil {
		t.Error(err)
	}

	nodes, err = result.Collect(ctx)
	if err != nil {
		t.Error("Failed to cast result to []*neo4j.Node")
	}

	if len(nodes) == 0 {
		t.Error("Expected nodes to be created")
	}

	if len(nodes) != 1 {
		t.Error("Expected 1 node to be created")
	}

	// check node id
	node = nodes[0].Values[0].(dbtype.Node)
	if node.Props["gid"] != "http://data.sample.org/things/mimiro" {
		t.Error("Expected node with gid http://data.sample.org/things/mimiro")
	}

	// check name property
	if node.Props["name"] != "Mimiro" {
		t.Error("Expected node with name Mimiro")
	}

	err = txn.Commit(ctx)
	if err != nil {
		t.Error(err)
	}

}

func makeEntity(id string) *egdm.Entity {
	entity := egdm.NewEntity().SetID("http://data.sample.org/things/" + id)
	entity.SetProperty("http://data.sample.org/name", "brian")
	entity.SetProperty("http://data.sample.org/age", 23)
	entity.SetReference("http://data.sample.org/worksfor", "http://data.sample.org/things/mimiro")
	return entity
}
