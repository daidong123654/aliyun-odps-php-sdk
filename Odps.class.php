<?php
namespace Odps;

use \ODPS\Core\OdpsClient;
use \ODPS\Services\FunctionService;
use \ODPS\Services\Udf;
use \ODPS\Services\ProjectService;
use \ODPS\Services\ResourceService;
use \ODPS\Services\TunnelService;
use \ODPS\Services\InstanceService;
use \ODPS\Services\TableService;
use \ODPS\Services\Tunnel\Protobuf\TableRecord;
# use \ODPS\Http;

class Odps{

    protected $odps;
    protected $functions;
    protected $tunnel;
    protected $instance;
    protected $project;
    protected $resource;
    protected $table;

    public function __construct($accessKeyId, $accessKeySecret, $endpoint = null, $defaultProject = null, $debug = false){
        require_once __DIR__ . '/autoload.php';
        $this->odps = new OdpsClient( $accessKeyId, $accessKeySecret, $endpoint, $defaultProject, $debug);
        if(is_object($this->odps)){
            # $this->functions = new FunctionService($this->odps);
            # $this->tunnel    = new TunnelService($this->odps);
            # $this->instance  = new InstanceService($this->odps);
            # $this->project   = new ProjectService($this->odps);
            # $this->resource  = new ResourceService($this->odps);
            # $this->table     = new TableService($this->odps);
        }else{
            throw new OdpsException("Error params");
        }
    }

    /**
     * 执行一个SQL 查询语句，采用临时表存储，之后自动删除
     * @param  $sql          SQL 语句
     * @param  $tmpTable     临时表名称没有分区
     * @param  $comment      备注
     * @param  $autoDelete   是否自动删除
     * @return $instanceId   任务instance id
     * */
    public function select($sql, $tmpTable, $comment='', $autoDelete=true){
        if($sql){
            // 1. instance obj
            $this->instance = new InstanceService($odps); 
            // 2. 获取任务
            $query = "CREATE TABLE {$tmpTable} AS {$sql};";
            $taskName  = "task_select_" . date('YmdH:i:s').uniqid();
            // 3. 执行并返回instance id
            $instanceId = $this->instance->postSqlTaskInstance($taskName, $comment, $query);
            if( is_string($instanceId) && !is_object($instanceId) ){
                // TODO 这里记录日志 
                // 4. 监听执行
                while( $this->instance->getInstance($instanceId) != 'Terminated' ){  # Terminated,Running,Suspended
                    sleep(1);
                }
                // 5. 下载数据
                $this->downloadTable($tmpTable);
                // 6. 删除临时表
            }
            return false;       # 创建失败
        }
        return false;
    }

    /**
     * 删除一张表 
     * @param $table   表名称
     * @param $project 项目
     * @return true/false
     * */
    public function dropTable($table, $project=''){
        if( $table ){
            // 1. instance obj 
            $this->instance = new InstanceService($odps);
            ( $project && $project != $this->odps->getDefaultProjectName() ) and $this->instance->SwitchProject($project);
            // 2. 执行SQL
            $sql = "DROP TABLE IF EXISTS `{$table}`;";
            $taskName  = "task_drop_" . date('YmdH:i:s').uniqid();
            // 3. 执行并返回instance id
            $instanceId = $this->instance->postSqlTaskInstance($taskName, $comment, $query);
            if( is_string($instanceId) && !is_object($instanceId) ){
                // TODO 这里记录日志 
                // 4. 监听执行,貌似不用
                # while( $this->instance->getInstance($instanceId) != 'Terminated' ){  # Terminated,Running,Suspended
                #    sleep(1);
                # }
                return $taskName;
            }
        }
        return false;
    }

    /**
     * 1. 根据分区下载一张表
     * @param $table      表名称
     * @param $partition  分区名称,可以为空
     * @param $project    项目名称
     * @param $space      每列之间分隔符
     * @param $yield      是否使用yield, 
     *                    如果使用Yiedl 外部用 foreach($this->downloadTable() as $line){ ...}
     *                    否则直接返回数组
     * @param $step       一次拉取多少条,默认1000条
     * */
    public function downloadTable($table, $partition='', $project='', $space=','){
        // 0. 参数检查
        if(
            $table && $partition &&
            $this->_checkTable($table, $project) && 
            ( ( $project && $this->_checkProject($project) ) || !$project) && 
            ( ( $partition && $this->_checkPartition($table, $partition, $project) ) || !$partition)
        ){
            try{
                // 1. 新建对象，并判断是否切换project
                $this->tunnel    = new TunnelService($this->odps);
                $project and $this->tunnel->SwitchProject($project);

                // 2. 创建 $downloadSession
                $downloadSession = $this->tunnel->createDownloadSession($table, $partition);
                $columnOjb       = $downloadSession->getColumns();                          # 表结构，字段
                $column          = json_decode(json_encode($columnOjb), true);              # 表结构，字段
                $tableKeyArr     = array_column($column, 'name');                           # 表结构列表
                $recordCount     = $downloadSession->getRecordCount();                      # 记录条数
                $recordReader    = $downloadSession->openRecordReader(0, $recordCount);     # 开始下载
                while ($record = $recordReader->read()) {
                    $tmpArr = [];
                    foreach ($record->getValues() as $k => $v) {
                        $tmpArr[] = $v;
                    }
                    yield array_combine($tableKeyArr, $tmpArr);
                }
            }catch(OdpsException $e){
                throw new OdpsException("Downloas Error" . $e);
            }
        }
    } 

    /**
     * 检查一个分区是否在一个表中
     * @param $table     表名称
     * @param $partition 分区,可以为空
     * @param $project   项目
     * */
    private function _checkPartition($table, $partition='', $project=''){
        $detail      = false;
        $skipProject = $skipTable = true;
        $partitionList = $this->_getPartitions($table, $project, $skipTable, $skipProject, $detail);
        return ($partition == '' and $partitionList===0) || in_array($partition, $partitionList);  # 一般返回0表示此表没有分区,这个不保证全是
    }

    /**
     * 检查一个项目下 table 是否存在
     * @param  $project 项目 
     * @param  $table   表名 
     * @param  $skipProject 是否直接跳过检查项目名称(默认此方法应该结合 _checkProject 使用)
     * @return true/false   
     * */
    private function _checkTable($table, $project='', $skipProject=true, $schemaNeed=true){
        // 1. 检查参数
        if( 
            $table && 
            ( $skipProject || $this->_checkProject($project) )
        ){
            // 3. 获取table 信息
            $this->table = new TableService($this->odps);
            $project and $this->table->SwitchProject($project);
            $tableObj = $this->table->getTable($table);
            // 2. 切换project 
            // 4. 处理
            if( $tableObj && $tableObj->header['_info']['http_code']==200 && !empty($tableObj->bodyArr) ){
                $bodyArr = $tableObj->bodyArr;
                $name   = $bodyArr['Name'];
                $schema = json_decode($bodyArr['Schema'], true);

                // 5. 判断并返回
                $res     = $name === $table ? ($schemaNeed ? [$name=>$schema] : true) : ($schemaNeed? [] : false);
                return $res;
            }
        }
        return false;
    }

    /**
     * 检查一个 project 是否存在
     * @param $project 项目名称
     * @param $status  是否返回状态，默认不返回
     * @param $status ? [@Name=>@Status] : false
     * */
    private function _checkProject($project, $status=false){
        // 1. 检查project 是否存在
        $this->project = new ProjectService($this->odps);
        $projStatusObj = $this->project->getProject($project) ? : [];
        // 2. 返回结果
        if( is_object($projStatusObj) && $projStatusObj->header['_info']['http_code'] == 200 && !empty($projStatusObj->bodyArr) ){
            $bodyArr = $projStatusObj->bodyArr;
            $name    = $bodyArr['Name'];
            $state   = $bodyArr['State'];
            $res     = $name === $project ? ($status ? [$name=>$state] : true) : ($status ? [] : false);
            return $res;
        }
        return $status ? [] : false;
    }

    /**
     * 获取一个表所有分区 
     * @param $table        表名称
     * @param $project      项目
     * @param $skipTable    是否跳过检查table
     * @param $skipProject  是否跳过检查project
     * @param $detail       是否需要详细信息默认不需要
     * @param return []
     * */
    private function _getPartitions($table, $project='', $skipTable=true, $skipProject=true, $detail=false){
        $res = ['list'=>[], 'detail'=>[]];
        // 1. 参数就检查
        if( 
            $table &&
            ( $skipTable || $this->_checkTable($project, $table)  ) &&
            ( $skipProject || $this->_checkProject($project) )
        ){
            // 2. 切换项目
            $this->table = new TableService($this->odps);
            $project and $this->table->SwitchProject($project);
            // 3. 获取 表 分区信息
            $partitionObj = $this->table->getTablePartitions($table);
            if( $partitionObj && $partitionObj->header['_info']['http_code'] == 200 && !empty($partitionObj->bodyArr) ){
                $bodyArr = $partitionObj->bodyArr;
                $maxItems = $bodyArr['MaxItems'];
                if($maxItems == 1){   // 只有一个分区(单列或多列) 11/12
                    $partition = $bodyArr['Partition'];
                    $column   = $partition['Column'];
                    if( 1==count($column) ){                        // 11
                        $attributes = $column['@attributes'];
                        $name  = $attributes['Name'];
                        $value = $attributes['Value'];
                        $res['detail'][] = [
                            'name'  => $name,
                            'value' => $value,
                            'partitions'   => "{$name}={$value}",
                            'creationTime' => $partition['CreationTime'],
                            'lastDDLTime'  => $partition['LastDDLTime'],
                            'lastModifiedTime' => $partition['LastModifiedTime'],
                        ];
                        $res['list'][] = "{$name}={$value}";
                    }else if( count($column) > 1) {         // 12
                        $tmpArr = [];
                        foreach ($column as $key => $colInfo) {
                            $attributes = $colInfo['@attributes'];
                            $tmpArr['name'][]  = $name  = $attributes['Name'];;
                            $tmpArr['value'][] = $value = $attributes['Value'];;
                            $tmpArr['creationTime'] = $partition['CreationTime'];
                            $tmpArr['lastDDLTime']  = $partition['LastDDLTime'];
                            $tmpArr['lastModifiedTime'] = $partition['LastModifiedTime'];
                            $tmpArr['partition']   .= ( $key == 0 or !trim($tmpArr['partition']) ) ? "{$name}={$value}" : ",{$name}={$value}";
                        }
                        $res['list'][]   = $tmpArr['partition'];
                        $res['detail'][] = $tmpArr;
                    }
                }else{          // 21/22
                    foreach ($bodyArr['Partition'] as $partition) {
                        $column = $partition['Column'];
                        if( 1==count($column) ){                    // 21
                            $attributes = $column['@attributes'];
                            $name  = $attributes['Name'];
                            $value = $attributes['Value'];
                            $res['detail'][] = [
                                'name'  => $name,
                                'value' => $value,
                                'partitions'   => "{$name}={$value}",
                                'creationTime' => $partition['CreationTime'],
                                'lastDDLTime'  => $partition['LastDDLTime'],
                                'lastModifiedTime' => $partition['LastModifiedTime'],
                            ];
                            $res['list'][] = "{$name}={$value}";
                        }else if( count($column) > 1) {             // 22
                            $tmpArr = [];
                            foreach ($column as $key => $colInfo) {
                                $attributes = $colInfo['@attributes'];
                                $tmpArr['name'][]  = $name  = $attributes['Name'];;
                                $tmpArr['value'][] = $value = $attributes['Value'];;
                                $tmpArr['creationTime'] = $partition['CreationTime'];
                                $tmpArr['lastDDLTime']  = $partition['LastDDLTime'];
                                $tmpArr['lastModifiedTime'] = $partition['LastModifiedTime'];
                                @$tmpArr['partition']   .= ( $key == 0 or !isset($tmpArr['partition']) ) ? "{$name}={$value}" : ",{$name}={$value}";
                            }
                            $res['list'][]   = $tmpArr['partition'];
                            $res['detail'][] = $tmpArr;
                        }
                    }    
                }
                return $detail ? $res['detail'] : $res['list'];
            }elseif( $partitionObj->header['_info']['http_code'] == 400 ){ # 一般400可能为此表本来就是临时表
                return 0;                
            }
        }
        return -1; 
    }
}




# $obj = new Odps();
// 1. 测试 下载数据 其中测试了
//  a._getPartitions
//  b._checkTable
//  c._checkProject
//  d._checkPartition
//  e.downloadTable
// foreach($obj->downloadTable('super_hjy_result', 'source=zmt,snapshot_id=20171219080002', 'aries_phase1') as $value){
# foreach($obj->downloadTable('scan_201510', 'day=31,hmid=46', 'aries_phase1') as $value){
#   echo implode(',', $value), PHP_EOL;
#}













