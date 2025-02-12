package layer

import (
	"context"
	"encoding/json"
	"fmt"
	cdl "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io/fs"
)

type OpenCypherDataLayer struct {
	config      *cdl.Config
	logger      cdl.Logger
	metrics     cdl.Metrics
	datasets    map[string]*GraphDataset
	graphSystem *GraphSystemConfig
}

type GraphQueryClient interface {
	Initialise(datasets []string) error
	DeleteAll(source string, label string) error
	WriteBatch(source string, label string, entities []*egdm.Entity) error
	Query(query string) (interface{}, error)
}

// GrahSystemConfig is the config for connecting to the graph database
type GraphSystemConfig struct {
	systemType string
	endpoint   string
	userName   string
	password   string
}

func NewOpenCypherDataLayer(conf *cdl.Config, logger cdl.Logger, metrics cdl.Metrics) (cdl.DataLayerService, error) {
	datalayer := &OpenCypherDataLayer{config: conf, logger: logger, metrics: metrics}

	err := datalayer.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}

	return datalayer, nil
}

func (dl *OpenCypherDataLayer) NewGraphQueryClient() (GraphQueryClient, error) {
	if dl.graphSystem.systemType == "neo4j" {
		client := NewNeo4jClient(dl.graphSystem.endpoint, dl.graphSystem.userName, dl.graphSystem.password, dl.logger)

		datasetNames := make([]string, 0, len(dl.config.DatasetDefinitions))
		for _, dataset := range dl.config.DatasetDefinitions {
			sourceConfig := dataset.SourceConfig
			label := sourceConfig["label"].(string)
			datasetNames = append(datasetNames, label)
		}

		err := client.Initialise(datasetNames)
		if err != nil {
			return nil, err
		}
		return client, nil
	} else {
		return nil, fmt.Errorf("unsupported system type %s", dl.graphSystem.systemType)
	}
}

func (dl *OpenCypherDataLayer) Stop(ctx context.Context) error {
	// noop
	return nil
}

func (dl *OpenCypherDataLayer) UpdateConfiguration(config *cdl.Config) cdl.LayerError {
	dl.config = config
	dl.datasets = make(map[string]*GraphDataset)

	// get connection details from native system
	nativeSystemConfig := config.NativeSystemConfig
	graphSystem := &GraphSystemConfig{}

	if nativeSystemConfig["system_type"] != nil {
		graphSystem.systemType = nativeSystemConfig["system_type"].(string)
	} else {
		return cdl.Err(fmt.Errorf("no system type specified in native system config"), cdl.LayerErrorBadParameter)
	}

	if nativeSystemConfig["endpoint"] != nil {
		graphSystem.endpoint = nativeSystemConfig["endpoint"].(string)
	} else {
		return cdl.Err(fmt.Errorf("no endpoint specified in native system config"), cdl.LayerErrorBadParameter)
	}

	if nativeSystemConfig["username"] != nil {
		graphSystem.userName = nativeSystemConfig["username"].(string)
	} else {
		return cdl.Err(fmt.Errorf("no username specified in native system config"), cdl.LayerErrorBadParameter)
	}

	if nativeSystemConfig["password"] != nil {
		graphSystem.password = nativeSystemConfig["password"].(string)
	} else {
		return cdl.Err(fmt.Errorf("no password specified in native system config"), cdl.LayerErrorBadParameter)
	}

	// configure the graph system
	dl.graphSystem = graphSystem

	// setup datasets
	queryClient, err := dl.NewGraphQueryClient()
	if err != nil {
		return cdl.Err(fmt.Errorf("could not create graph query client because %s", err.Error()), cdl.LayerErrorInternal)
	}
	for _, dataset := range config.DatasetDefinitions {
		dl.datasets[dataset.DatasetName], err =
			NewGraphDataset(dataset.DatasetName, queryClient, dataset, dl.logger)
		if err != nil {
			return cdl.Err(fmt.Errorf("could not create dataset %s because %s", dataset.DatasetName, err.Error()), cdl.LayerErrorInternal)
		}
	}

	return nil
}

func (dl *OpenCypherDataLayer) Dataset(dataset string) (cdl.Dataset, cdl.LayerError) {
	dl.logger.Info(fmt.Sprintf("get dataset %s", dataset))
	ds, ok := dl.datasets[dataset]
	if !ok {
		return nil, cdl.Err(fmt.Errorf("dataset %s not found", dataset), cdl.LayerErrorBadParameter)
	}

	return ds, nil
}

func (dl *OpenCypherDataLayer) DatasetDescriptions() []*cdl.DatasetDescription {
	dl.logger.Info("get dataset descriptions")
	var datasetDescriptions []*cdl.DatasetDescription

	// iterate over the datasest testconfig and create one for each
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &cdl.DatasetDescription{Name: key})
	}

	return datasetDescriptions
}

func NewGraphDatasetConfig(sourceConfig map[string]any) (*GraphDatasetConfig, error) {
	data, err := json.Marshal(sourceConfig)
	if err != nil {
		return nil, err
	}

	config := &GraphDatasetConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

type GraphDatasetConfig struct {
	BatchSize int    `json:"batch_size"`
	Label     string `json:"label"`
}

func NewGraphDataset(name string, queryClient GraphQueryClient, datasetDefinition *cdl.DatasetDefinition, logger cdl.Logger) (*GraphDataset, error) {
	sourceConfig := datasetDefinition.SourceConfig

	config, err := NewGraphDatasetConfig(sourceConfig)
	if err != nil {
		return nil, err
	}

	return &GraphDataset{name: name,
		config:            config,
		datasetDefinition: datasetDefinition,
		logger:            logger,
		queryClient:       queryClient}, nil
}

type GraphDataset struct {
	logger            cdl.Logger
	name              string                 // dataset name
	datasetDefinition *cdl.DatasetDefinition // the dataset definition with mappings etc
	config            *GraphDatasetConfig    // the dataset config
	queryClient       GraphQueryClient       // the query client
}

func (f *GraphDataset) MetaData() map[string]any {
	return make(map[string]any)
}

func (f *GraphDataset) Name() string {
	return f.name
}

func (f *GraphDataset) FullSync(ctx context.Context, batchInfo cdl.BatchInfo) (cdl.DatasetWriter, cdl.LayerError) {
	f.logger.Info(fmt.Sprintf("full sync for dataset %s", f.name))
	if batchInfo.IsStartBatch {
		f.logger.Debug(fmt.Sprintf("start batch full sync for dataset %s", f.name))
		// delete all data in the graph with this dataset name source
		err := f.queryClient.DeleteAll(f.name, f.config.Label)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not delete all data in the graph because %s", err.Error()), cdl.LayerErrorInternal)
		}
	}

	datasetWriter := &CypherDatasetWriter{logger: f.logger, GraphQueryClient: f.queryClient, datasetName: f.name, label: f.config.Label, BatchSize: f.config.BatchSize, toWrite: make([]*egdm.Entity, 0)}
	return datasetWriter, nil
}

func (f *GraphDataset) Incremental(ctx context.Context) (cdl.DatasetWriter, cdl.LayerError) {
	f.logger.Info(fmt.Sprintf("incremental sync for dataset %s", f.name))
	datasetWriter := &CypherDatasetWriter{logger: f.logger, GraphQueryClient: f.queryClient, datasetName: f.name, label: f.config.Label, BatchSize: f.config.BatchSize, toWrite: make([]*egdm.Entity, 0)}
	return datasetWriter, nil
}

type CypherDatasetWriter struct {
	logger           cdl.Logger
	closeFullSync    bool
	GraphQueryClient GraphQueryClient
	BatchSize        int
	toWrite          []*egdm.Entity
	label            string
	datasetName      string
}

func (f *CypherDatasetWriter) Write(entity *egdm.Entity) cdl.LayerError {
	f.toWrite = append(f.toWrite, entity)
	if len(f.toWrite) >= f.BatchSize {
		f.logger.Debug(fmt.Sprintf("writing batch of %d entities to dataset %s", len(f.toWrite), f.datasetName))
		err := f.GraphQueryClient.WriteBatch(f.datasetName, f.label, f.toWrite)
		if err != nil {
			return cdl.Err(fmt.Errorf("could not write batch because %s", err.Error()), cdl.LayerErrorInternal)
		}
		f.toWrite = make([]*egdm.Entity, 0)
	}
	return nil
}

func (f *CypherDatasetWriter) Close() cdl.LayerError {
	f.logger.Info(fmt.Sprintf("closing dataset writer for dataset %s", f.datasetName))
	if len(f.toWrite) > 0 {
		f.logger.Debug(fmt.Sprintf("writing batch of %d entities to dataset %s", len(f.toWrite), f.datasetName))
		err := f.GraphQueryClient.WriteBatch(f.datasetName, f.label, f.toWrite)
		if err != nil {
			return cdl.Err(fmt.Errorf("could not write batch because %s", err.Error()), cdl.LayerErrorInternal)
		}
	}
	return nil
}

type FileInfo struct {
	Entry fs.DirEntry
	Path  string
}

func (f *GraphDataset) Changes(since string, limit int, latestOnly bool) (cdl.EntityIterator, cdl.LayerError) {
	f.logger.Info(fmt.Sprintf("get changes for dataset %s", f.name))
	return nil, cdl.Err(fmt.Errorf("operation not supported"), cdl.LayerNotSupported)
}

func (f *GraphDataset) Entities(from string, limit int) (cdl.EntityIterator, cdl.LayerError) {
	f.logger.Info(fmt.Sprintf("get entities for dataset %s", f.name))
	return nil, cdl.Err(fmt.Errorf("operation not supported"), cdl.LayerNotSupported)
}
