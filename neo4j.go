package layer

import (
	"context"
	"fmt"
	cdl "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"strings"
	"time"
)

type Neo4jClient struct {
	endpoint string
	username string
	password string
	realm    string
	logger   cdl.Logger
}

const IndexQuery = "CREATE INDEX external_id_index_%s IF NOT EXISTS FOR (n:%s) ON (n.gid)"

func (n *Neo4jClient) Initialise(datasets []string) error {
	n.logger.Info("initialising neo4j client", "datasets", datasets)
	driver, err := n.Connect()
	if err != nil {
		return err
	}

	ctx := context.Background()
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	txn, err := session.BeginTransaction(ctx, func(config *neo4j.TransactionConfig) { config.Timeout = 15 * time.Minute })
	if err != nil {
		return err
	}

	for _, dataset := range datasets {
		n.logger.Debug("creating index", "dataset", dataset)
		res, err := txn.Run(ctx, fmt.Sprintf(IndexQuery, dataset, dataset), nil)
		fmt.Println(res)
		if err != nil {
			return err
		}
	}

	err = txn.Commit(ctx)

	return nil
}

func NewNeo4jClient(endpoint string, username string, password string, logger cdl.Logger) *Neo4jClient {
	return &Neo4jClient{
		endpoint: endpoint,
		username: username,
		password: password,
		logger:   logger,
	}
}

type Neo4jLogger struct {
	logger cdl.Logger
}

func (n *Neo4jLogger) Error(name, msg string, err error) {
	n.logger.Error(fmt.Sprintf("%s: %s", name, msg), "error", err)
}

func (n *Neo4jLogger) Warn(name, msg string) {
	n.logger.Warn(fmt.Sprintf("%s: %s", name, msg))
}

func (n *Neo4jLogger) Info(name, msg string) {
	n.logger.Info(fmt.Sprintf("%s: %s", name, msg))
}

func (n *Neo4jLogger) Debug(name, msg string) {
	n.logger.Debug(fmt.Sprintf("%s: %s", name, msg))
}

func (n *Neo4jClient) Connect() (neo4j.DriverWithContext, error) {
	dbUri := n.endpoint // scheme://host(:port) (default port is 7687)
	driver, err := neo4j.NewDriverWithContext(dbUri, neo4j.BasicAuth(n.username, n.password, n.realm))
	if err != nil {
		n.logger.Error("Failed to connect to Neo4j", "error", err)
		return nil, err
	}
	n.logger.Info("Successfully connected to Neo4j")
	return driver, nil
}

const DeleteNodeQueryTemplate = `
UNWIND $items AS item
MATCH (n {gid: item.gid})
DETACH DELETE n
`

const UpdateNodeQueryTemplate = `
UNWIND $items AS item
MERGE (n {gid: item.gid})
WITH n, item
OPTIONAL MATCH (n)-[r]->()
DELETE r
SET n:%s
SET n = item
`

const TargetNodeQueryTemplate = `
UNWIND $items AS item
MERGE (n {gid: item.gid })
`

const UpdateEdgeQueryTemplate = `
UNWIND $items AS item
MATCH (n1 {gid: item.from})
MATCH (n2 {gid: item.to})
MERGE (n1)-[r:%s]->(n2)
SET r.source = item.source
SET r.type = item.type
`

const DeleteAllBySourceAndLabelTemplate = `
MATCH (n:%s {source: "%s"}) DETACH DELETE n
`

// given a URI return the last part after # or /
func stripPrefix(s string) string {
	if i := strings.LastIndex(s, "#"); i != -1 {
		return s[i+1:]
	}
	if i := strings.LastIndex(s, "/"); i != -1 {
		return s[i+1:]
	}
	return s
}

func (n *Neo4jClient) DeleteAll(source string, label string) error {
	n.logger.Info("deleting all nodes", "source", source, "label", label)
	driver, err := n.Connect()
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	txn, err := session.BeginTransaction(ctx, func(config *neo4j.TransactionConfig) { config.Timeout = 15 * time.Minute })

	if err != nil {
		return err
	}

	_, err = txn.Run(ctx, fmt.Sprintf(DeleteAllBySourceAndLabelTemplate, label, source), nil)
	if err != nil {
		return err
	}

	err = txn.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (n *Neo4jClient) WriteBatch(source string, label string, entities []*egdm.Entity) error {
	n.logger.Info("writing batch", "source", source, "label", label, "entities", len(entities))
	driver, err := n.Connect()
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer driver.Close(ctx)

	// nodeItems for updates
	deletedItems := make([]map[string]interface{}, 0)
	nodeItems := make([]map[string]interface{}, 0)
	listOfTargetNodes := make(map[string]string, 0)
	relationshipsItems := make(map[string][]map[string]interface{}, 0)

	for _, entity := range entities {
		if entity.IsDeleted {
			deletedItems = append(deletedItems, map[string]interface{}{"gid": entity.ID})
			continue
		}

		itemMap := make(map[string]interface{})
		itemMap["gid"] = entity.ID
		itemMap["source"] = source
		for k, v := range entity.Properties {
			itemMap[stripPrefix(k)] = v
		}

		for property, rel := range entity.References {
			related := make([]string, 0)
			switch val := rel.(type) {
			case string:
				related = append(related, val)
			case []string:
				related = append(related, val...)
			default:
				return fmt.Errorf("unsupported type %T", val)
			}

			for _, target := range related {
				if _, ok := listOfTargetNodes[target]; !ok {
					listOfTargetNodes[target] = target
				}

				relItem := map[string]interface{}{
					"from":   entity.ID,
					"to":     target,
					"rel":    stripPrefix(property),
					"source": source,
				}

				if _, ok := relationshipsItems[property]; !ok {
					relationshipsItems[property] = make([]map[string]interface{}, 0)
				}

				relationshipsItems[property] = append(relationshipsItems[property], relItem)
			}
		}

		// add to all nodeItems
		nodeItems = append(nodeItems, itemMap)
	}

	// start a txn then using the templates do the needful
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	txn, err := session.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	// delete nodes
	if len(deletedItems) > 0 {
		_, err = txn.Run(ctx, DeleteNodeQueryTemplate, map[string]interface{}{"items": deletedItems})
		if err != nil {
			return err
		}
	}

	// update nodes
	if len(nodeItems) > 0 {
		_, err = txn.Run(ctx, fmt.Sprintf(UpdateNodeQueryTemplate, label), map[string]interface{}{"items": nodeItems})
		if err != nil {
			return err
		}
	}

	// create target nodes
	if len(listOfTargetNodes) > 0 {
		// make item list
		items := make([]map[string]any, 0)
		for k := range listOfTargetNodes {
			items = append(items, map[string]any{"gid": k})
		}

		_, err = txn.Run(ctx, TargetNodeQueryTemplate, map[string]interface{}{"items": items})
		if err != nil {
			return err
		}
	}

	// update relationships
	for rel, items := range relationshipsItems {
		_, err = txn.Run(ctx, fmt.Sprintf(UpdateEdgeQueryTemplate, stripPrefix(rel)), map[string]interface{}{"items": items})
		if err != nil {
			return err
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (n *Neo4jClient) Query(query string) (interface{}, error) {
	return nil, nil
}
