<?php
/**************************************************
 *    Filename:      test.php
 *    Copyright:     (C) 2017 All rights reserved
 *    Author:        Theast
 *    Email:         Daidong123654@126.com
 *    Description:   ---
 *    Create:        2017-12-14 23:35:33
 *    Last Modified: 2017-12-14 23:37:30
 *************************************************/ 
use \ODPS\Core\OdpsClient;
use \ODPS\Services\FunctionService;
use \ODPS\Services\Udf;
use \ODPS\Services\ProjectService;
use \ODPS\Services\TunnelService;
use \ODPS\Services\InstanceService;
use \ODPS\Services\TableService;
use \ODPS\Services\Tunnel\Protobuf\TableRecord;

require_once __DIR__ . "/autoload.php";


$odps->setDebugMode(true);


# table service
$tableService = new TableService($odps);
// $tables = $tableService->getTables();
// foreach ($tables as $table) {
// 	break;
//     print  $table->Name . "\r\n";
// }

# download service
// $tableService->SwitchProject('aries_phase1');
// $parations = $tableService->getTablePartitions('super_hjy_zmt_matched');
// var_dump($parations);die;

// $table = $tableService->getTable('tmp_super_hjy_zmt_matched');
// var_dump($table);die;
// // var_dump($tableService->getTable('a'));
// die;

# tunnel service
$tunnel =  new TunnelService($odps);
$tunnel->setCurrProject("demo_daidong");
// var_dump($tunnel);
// $downloadSession = $tunnel->createDownloadSession("tmp_super_hjy_zmt_matched", "snapshot_id=20171213142838");
// $downloadSession = $tunnel->createDownloadSession("tmp_super_hjy_result", 'source=zmt,snapshot_id=20171213142838');
// $recordCount = $downloadSession->getRecordCount();
// print "\r\nTotal record count:" . $recordCount . "\r\n";
// $recordReader = $downloadSession->openRecordReader(0, $recordCount);
// $index = 1;
// while ($record = $recordReader->read()) {
//     // print $index++ . "\n Record Start: " . "\n";
//     foreach ($record->getValues() as $v) {

//         // if ($v instanceof DateTime) {
//         //     print $v->format("Y-m-d\TH:i:s\Z");
//         // } else {
//         //     print $v;
//         // }
//         print $v;
//         print ", ";
//     }
//     print "\n";
//     // print "\n Record Done: " . "\n";
// }
// // $recordReader->close();
// var_dump($downloadSession->getColumns());
// die;


# upload
# ALTER TABLE test_table ADD IF NOT EXISTS PARTITION ('20171216');
$tableName = 'test_table';
$partion   = 'dt=20171216';
$uploadSerssion = $tunnel->createUploadSession($tableName, $partion);
$recordWriter = $uploadSerssion->openRecordWriter(0);
$cols = $uploadSerssion->getColumns();
var_dump($cols);
die;
var_dump('expression');
$record = new TableRecord();
var_dump($record);die;
for ($i = 0; $i < sizeof($cols); $i++) {
    $col = $cols[$i];
    $pbIndex = $i + 1;
    switch ($col->type) {
        case "string":
            $record->setColumnValue($pbIndex, $col->type, "sample122222223");
            break;
        case "bigint":
            $record->setColumnValue($pbIndex, $col->type, pow(2, 33));
            break;
        case "boolean":
            $record->setColumnValue($pbIndex, $col->type, true);
            break;
        case "double":
            $record->setColumnValue($pbIndex, $col->type, -0.3);
            break;
        case "datetime":
            $record->setColumnValue($pbIndex, $col->type, (new DateTime()));
            break;
        default:
            throw new Exception("Unknown column type:" . $col->type);
    }
}
for ($i = 0; $i < 100; $i++) {
    $recordWriter->write($record);
}
$ret = $recordWriter->close();
$uploadSerssion->commit();
print "Upload done";
die;


# sql service
$instance = new InstanceService($odps);

// 获取所有 instance
// $daterange = '20171214:';
// $status    = 'Terminated';
// // $allInstances = $instance->getInstancesNew($daterange, $status);
// $allInstances = $instance->getInstances($daterange, $status);
// // var_dump($allInstances->rewind());
// $allInstances->rewind();
// while ($allInstances->valid()){
//     // $key = $allInstances->key();
//     $value = $allInstances->current();
//     $allInstances->next();
//     var_dump($value);
// }



$sql = "SELECT * from(
    SELECT *, rank() OVER (PARTITION BY t1.mid, t1.dt ORDER BY t1.updated_at DESC) AS rnk
            FROM zm_stat.yunac_stat_visitors_daily t1
            WHERE t1.dt = 20171104 and t1.mid=66404
) t2 where rnk=1 limit 10;";
// 创建任务
$taskName = 'task' . date('YmdHis') . '' . uniqid();
$instanceId = $instance->postSqlTaskInstance($taskName, null, $sql);	# 初始化任务并得到任务id
print "taskName: {$taskName}\tinstanceId: ". $instanceId . PHP_EOL;

echo "progress" . PHP_EOL;
$progress = $instance->getInstanceProgress($instanceId, $taskName);
var_dump($progress->body);

// 检查执行完毕
$status = 'Running';
while ($status == 'Running') {
	$obj = $instance->getInstance($instanceId);
	$status = $obj->Status;
	var_dump($status);
	var_dump( $instance->getInstanceTask($instanceId)->bodyArr);
	sleep(1);
}

$taskStatus = $instance->getInstanceTask($instanceId);
var_dump($taskStatus->bodyArr['Tasks']['Task']['Status']);

// 查看进度
echo "progress" . PHP_EOL;
$progress = $instance->getInstanceProgress($instanceId, $taskName);
var_dump($progress->bodyArr);

echo "detail" . PHP_EOL;
$detail = $instance->getInstanceDetail($instanceId, $taskName);
var_dump($detail->bodyArr);
foreach (json_decode($detail->body) as $value) {
	var_dump($value);
}
die;
// echo "detail body" . PHP_EOL;
// var_dump($detail->debugInfo);

echo "summary" . PHP_EOL;
$summary = $instance->getInstanceSummary($instanceId, $taskName);
// var_dump($summary->header);
var_dump($summary->body);
// var_dump($summary->status);
var_dump($summary->isOK());









while ($record = $recordReader->read()) {
    foreach ($record->getValues() as $v) {
        print $v;
        print ", ";
    }
    print "\n";
}