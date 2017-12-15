<?php

namespace ODPS\Services;

use ODPS\Core\OdpsBase;
use ODPS\Core\ResourceBuilder;
use ODPS\Core\OdpsClient;

/**
 * Class Table provides all operations for odps Table
 *
 * @package ODPS\Services
 */
class TableService extends \ODPS\Core\OdpsBase {

    /**
     * get table 操作用于查看某 table 的扩展信息。
     * @param $tableName 表名称
     * @return 
     *      Name        table 名称
     *      TableId     系统为 table 生成的唯一 ID 值
     *      Comment     table 备注信息
     *      Schema      table 的扩展描述信息，以 json 格式呈现, 具体内容为 table 的扩展信息，参见示例
     * */
    public function getExtendedTable($tableName) {
        $resource = ResourceBuilder::buildTableUrl($this->getDefaultProjectName(), $tableName);
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_SUB_RESOURCE => "extended"
        );
        return $this->call($options);
    }

    /**
     * get table 操作用于查看某 table 信息。
     * @param $tableName 表名
     * @return <?xml version="1.0" encoding="UTF-8"?>
     * <Table>
     *     <Name>tablename</Name>
     *     <TableId>tableId</TableId>
     *     <Comment>table comment</Comment>
     *     <Schema format="Json">
     *     {
     *     "columns": [],
     *     "comment": "",
     *     "createTime": timestamp,
     *     "hubLifecycle": -1,
     *     "isVirtualView": false,
     *     "lastDDLTime": timestamp,
     *     "lastModifiedTime": timestamp,
     *     "lifecycle": -1,
     *     "owner": ""
     *     "partitionKeys": [],
     *     "shardExist": false,
     *     "size": 0,
     *     "tableLabel": "",
     *     "tableName": ""
     *     }
     *     </Schema>
     * </Table> 
     * */
    public function getTable($tableName) {
        $resource = ResourceBuilder::buildTableUrl($this->getDefaultProjectName(), $tableName);
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource
        );
        return $this->call($options);
    }

    /**
     * get table partitions 操作用于查看表的所有 partition 信息。
     * @param $tableName 表名称
     * @return <?xml version="1.0" ?>
     * <Partitions>
     *     <Marker/>
     *     <MaxItems>ItemNumber</MaxItems>
     *     <Partition>
     *     <Column Name="" Value=""/>
     *     <Column Name="" Value=""/>
     *     </Partition>
     *     <Partition>
     *     <Column Name="" Value=""/>
     *     <Column Name="" Value=""/>
     *     </Partition>
     *  </Partitions>
     * */
    public function getTablePartitions($tableName) {
        $resource = ResourceBuilder::buildTableUrl($this->getDefaultProjectName(), $tableName);
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_SUB_RESOURCE => "partitions"
        );
        return $this->call($options);
    }

    /**
     * 获取所有表名称，迭代器使用 
     * @param $paras 参见 getTables 
     * */
    public function getTablesInternal($paras) {
        $resource = ResourceBuilder::buildTablesUrl($this->getDefaultProjectName());
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_PARAMS => $paras
        );
        $rst = $this->call($options);
        return $rst;
    }

    /**
     * 获取所有表信息
     * @param $tableName 表名称
     * @param $owner 指定tables的所有者作为查询条件，查询条件为表的owner=owner_name。
     *               参数可省，默认所有tables； 
     * @return OdpsIterator
     * */
    public function getTables($tableName = null, $owner = null) {
        return new OdpsIterator(
            array(
                "obj" => $this,
                "func" => "getTablesInternal",
                "funcParams" => array(
                    "expectmarker" => "true",
                    "name" => $tableName,
                    "owner" => $owner
                ),
                "itemName" => "Table"
            )
        );
    }
}
