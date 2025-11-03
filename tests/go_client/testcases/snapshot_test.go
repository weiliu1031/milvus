package testcases

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

var snapshotPrefix = "snapshot"

// TestCreateSnapshot tests creating a snapshot for a collection
func TestCreateSnapshot(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create a collection first
	collName := common.GenRandomString(snapshotPrefix, 6)
	err := mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
	common.CheckErr(t, err, true)

	// Get collection schema and insert data
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	prepare, _ := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, collName)

	// Create snapshot
	snapshotName := fmt.Sprintf("snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Test snapshot for e2e testing")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created by listing snapshots
	listOpt := client.NewListSnapshotsOption().
		WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Describe the snapshot
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	resp, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, resp.GetName())
	require.Equal(t, collName, resp.GetCollectionName())
	require.Equal(t, "Test snapshot for e2e testing", resp.GetDescription())
	require.Greater(t, resp.GetCreateTs(), int64(0))

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiSegment tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithMultiSegment(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 30000
	deleteBatchSize := 10000
	numOfBatch := 5

	// Step 1: Create collection and insert initial 3000 records
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	schema.WithShardNum(10)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Insert records
	for i := 0; i < numOfBatch; i++ {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}
	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(10 * time.Second)

	// Verify initial data count
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(insertBatchSize*numOfBatch), count)

	// Delete records
	for i := 0; i < numOfBatch; i++ {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	time.Sleep(10 * time.Second)

	// Verify data count after deletion
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(100000), count)

	// Step 2: Create snapshot
	snapshotName := fmt.Sprintf("restore_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for restore testing with 2000 records")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("check snapshot info", zap.Any("info", snapshotInfo))

	// Step 3: Continue inserting more records and delete 1000 records
	// Insert more records
	for i := 0; i < numOfBatch; i++ {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Verify total data count after second insertion
	queryRes3, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(250000), count)

	// Step 4: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	restoreJobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for a while to ensure restore is completed
	for {
		client.NewGetRestoreSnapshotStateOption(restoreJobID)
		state, err := mc.GetRestoreSnapshotState(ctx, client.NewGetRestoreSnapshotStateOption(restoreJobID))
		common.CheckErr(t, err, true)
		log.Info("restore snapshot state", zap.Any("state", state))
		if state.Progress == 100 {
			break
		}

		if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted {
			break
		}

		if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotFailed {
			t.Fatalf("restore snapshot failed, reason: %s", state.GetReason())
		}
		time.Sleep(1 * time.Second)
	}

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// load restored collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Verify restored partition data count
	queryRes5, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes5.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(100000), count)

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiShardMultiPartition tests the complete snapshot restore workflow with data operations
func TestSnapshotRestoreWithMultiShardMultiPartition(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 3000
	deleteBatchSize := 1000

	// Step 1: Create collection and insert initial 3000 records
	collName := common.GenRandomString(snapshotPrefix, 6)
	schema := client.SimpleCreateCollectionOptions(collName, common.DefaultDim)
	schema.WithAutoID(false)
	schema.WithShardNum(3)
	err := mc.CreateCollection(ctx, schema)
	common.CheckErr(t, err, true)

	partitions := make([]string, 0)
	for i := 0; i < 10; i++ {
		partitions = append(partitions, fmt.Sprintf("part_%d", i))
		option := client.NewCreatePartitionOption(collName, partitions[i])
		err := mc.CreatePartition(ctx, option)
		common.CheckErr(t, err, true)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})

	// Get collection schema
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Insert records
	for i, partition := range partitions {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema).TWithPartitionName(partition), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Verify initial data count
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(30000), count)

	// Delete records
	for i := range partitions {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(1000), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	time.Sleep(10 * time.Second)

	// Verify data count after deletion
	queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(20000), count)

	// Step 2: Create snapshot
	snapshotName := fmt.Sprintf("restore_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for restore testing with 2000 records")

	err = mc.CreateSnapshot(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("check snapshot info", zap.Any("info", snapshotInfo))

	// Step 3: Continue inserting more records and delete 1000 records
	// Insert more records
	for i, partition := range partitions {
		pkStart := insertBatchSize * (len(partitions) + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema).TWithPartitionName(partition), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}

	// Verify total data count after second insertion
	queryRes3, err := mc.Query(ctx, client.NewQueryOption(collName).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = queryRes3.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(50000), count)

	// Step 4: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	restoreJobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for a while to ensure restore is completed
	for {
		client.NewGetRestoreSnapshotStateOption(restoreJobID)
		state, err := mc.GetRestoreSnapshotState(ctx, client.NewGetRestoreSnapshotStateOption(restoreJobID))
		common.CheckErr(t, err, true)
		log.Info("restore snapshot state", zap.Any("state", state))
		if state.Progress == 100 {
			break
		}

		if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted {
			break
		}

		if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotFailed {
			t.Fatalf("restore snapshot failed, reason: %s", state.GetReason())
		}
		time.Sleep(1 * time.Second)
	}

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// load restored collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	for _, partition := range partitions {
		// Verify restored partition data count (should be 2000 records from snapshot)
		queryRes5, err := mc.Query(ctx,
			client.NewQueryOption(restoredCollName).
				WithOutputFields(common.QueryCountFieldName).
				WithConsistencyLevel(entity.ClStrong).
				WithPartitions(partition))
		common.CheckErr(t, err, true)
		count, _ = queryRes5.Fields[0].GetAsInt64(0)
		require.Equal(t, int64(2000), count)
	}

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}

// TestSnapshotRestoreWithMultiFields tests snapshot restore with all supported field types
func TestSnapshotRestoreWithMultiFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	insertBatchSize := 10000
	deleteBatchSize := 3000
	numOfBatch := 5

	// Step 1: Create collection with all field types
	collName := common.GenRandomString(snapshotPrefix, 6)

	// Create schema with all supported field types
	pkField := entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true)

	// Scalar fields
	boolField := entity.NewField().WithName("bool_field").WithDataType(entity.FieldTypeBool)
	int64Field := entity.NewField().WithName("int64_field").WithDataType(entity.FieldTypeInt64)
	floatField := entity.NewField().WithName("float_field").WithDataType(entity.FieldTypeFloat)
	varcharField := entity.NewField().WithName("varchar_field").WithDataType(entity.FieldTypeVarChar).WithMaxLength(200)
	jsonField := entity.NewField().WithName("json_field").WithDataType(entity.FieldTypeJSON)

	floatVecField := entity.NewField().WithName("float_vec").WithDataType(entity.FieldTypeFloatVector).WithDim(128)

	// Array fields - representative types
	int64ArrayField := entity.NewField().WithName("int64_array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(100)
	stringArrayField := entity.NewField().WithName("string_array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxLength(50).WithMaxCapacity(100)

	// Create schema
	schema := entity.NewSchema().
		WithName(collName).
		WithField(pkField).
		WithField(boolField).
		WithField(int64Field).
		WithField(floatField).
		WithField(varcharField).
		WithField(jsonField).
		WithField(floatVecField).
		WithField(int64ArrayField).
		WithField(stringArrayField).
		WithDynamicFieldEnabled(true)

	// Create collection with 5 shards
	createOpt := client.NewCreateCollectionOption(collName, schema).WithShardNum(5)
	err := mc.CreateCollection(ctx, createOpt)
	common.CheckErr(t, err, true)

	// Get collection schema for data insertion
	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	// Step 2a: Create indexes for vector field (required before loading)
	log.Info("Creating index for vector field")
	vecIdx := index.NewHNSWIndex(entity.L2, 8, 96)
	vecIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "float_vec", vecIdx))
	common.CheckErr(t, err, true)
	err = vecIndexTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 2b: Create indexes for scalar fields to accelerate filtering
	log.Info("Creating indexes for scalar fields")
	scalarIndexFields := []string{"int64_field", "varchar_field"}
	for _, fieldName := range scalarIndexFields {
		scalarIdx := index.NewInvertedIndex()
		scalarIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, scalarIdx))
		common.CheckErr(t, err, true)
		err = scalarIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 2c: Create indexes for array fields
	log.Info("Creating indexes for array fields")
	arrayIndexFields := []string{"int64_array", "string_array"}
	for _, fieldName := range arrayIndexFields {
		arrayIdx := index.NewInvertedIndex()
		arrayIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, arrayIdx))
		common.CheckErr(t, err, true)
		err = arrayIndexTask.Await(ctx)
		common.CheckErr(t, err, true)
	}

	// Step 2d: Load collection
	log.Info("Loading collection")
	loadOpt := client.NewLoadCollectionOption(collName).WithReplica(1)
	loadTask, err := mc.LoadCollection(ctx, loadOpt)
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Step 2e: Insert first batch of data (5 batches × 10,000 records)
	for i := 0; i < numOfBatch; i++ {
		insertOpt := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(i * insertBatchSize)
		_, insertRes := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt)
		require.Equal(t, insertBatchSize, insertRes.IDs.Len())
	}

	// Flush to ensure data is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Wait for flush to complete
	time.Sleep(10 * time.Second)

	// Step 3: Delete some records (3,000 from each batch)
	for i := 0; i < numOfBatch; i++ {
		deleteExpr := fmt.Sprintf("id >= %d and id < %d", insertBatchSize*i, insertBatchSize*i+deleteBatchSize)
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(deleteExpr))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(deleteBatchSize), delRes.DeleteCount)
	}

	// Flush to ensure deletion is persisted
	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Wait for flush to complete
	time.Sleep(10 * time.Second)

	// Step 4: Create snapshot
	snapshotName := fmt.Sprintf("multi_fields_snapshot_%s", common.GenRandomString(snapshotPrefix, 6))
	createSnapshotOpt := client.NewCreateSnapshotOption(snapshotName, collName).
		WithDescription("Snapshot for multi-fields restore testing")

	err = mc.CreateSnapshot(ctx, createSnapshotOpt)
	common.CheckErr(t, err, true)

	// Verify snapshot was created
	listOpt := client.NewListSnapshotsOption().WithCollectionName(collName)
	snapshots, err := mc.ListSnapshots(ctx, listOpt)
	common.CheckErr(t, err, true)
	require.Contains(t, snapshots, snapshotName)

	// Print snapshot info
	describeOpt := client.NewDescribeSnapshotOption(snapshotName)
	snapshotInfo, err := mc.DescribeSnapshot(ctx, describeOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, snapshotName, snapshotInfo.GetName())
	log.Info("Created snapshot for multi-fields test", zap.Any("info", snapshotInfo))

	// Step 5: Continue inserting more records (3 batches × 10,000 records)
	// This is to verify that snapshot captures state before these insertions
	for i := 0; i < 3; i++ {
		pkStart := insertBatchSize * (numOfBatch + i)
		insertOpt2 := hp.TNewDataOption().TWithNb(insertBatchSize).TWithStart(pkStart)
		_, insertRes2 := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), insertOpt2)
		require.Equal(t, insertBatchSize, insertRes2.IDs.Len())
	}
	// Total data after this step: 35,000 + 30,000 = 65,000
	// But snapshot should restore only 35,000 records

	// Step 6: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", collName)
	restoreOpt := client.NewRestoreSnapshotOption(snapshotName, restoredCollName)
	restoreJobID, err := mc.RestoreSnapshot(ctx, restoreOpt)
	common.CheckErr(t, err, true)

	// Wait for restore to complete
	for {
		state, err := mc.GetRestoreSnapshotState(ctx, client.NewGetRestoreSnapshotStateOption(restoreJobID))
		common.CheckErr(t, err, true)
		log.Info("Restore snapshot state", zap.Any("state", state))

		if state.Progress == 100 {
			break
		}

		if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted {
			break
		}

		if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotFailed {
			t.Fatalf("restore snapshot failed, reason: %s", state.GetReason())
		}
		time.Sleep(1 * time.Second)
	}

	// Verify restored collection exists
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	// Load restored collection
	loadTask, err = mc.LoadCollection(ctx, client.NewLoadCollectionOption(restoredCollName).WithReplica(1))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Verify restored collection data count (should be 35,000 from snapshot)
	queryRes, err := mc.Query(ctx,
		client.NewQueryOption(restoredCollName).
			WithOutputFields(common.QueryCountFieldName).
			WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := queryRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(35000), count)

	// Verify schema of restored collection
	restoredColl, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(restoredCollName))
	common.CheckErr(t, err, true)
	require.Equal(t, len(coll.Schema.Fields), len(restoredColl.Schema.Fields))
	require.True(t, restoredColl.Schema.EnableDynamicField)

	// Clean up
	dropOpt := client.NewDropSnapshotOption(snapshotName)
	err = mc.DropSnapshot(ctx, dropOpt)
	common.CheckErr(t, err, true)
}
