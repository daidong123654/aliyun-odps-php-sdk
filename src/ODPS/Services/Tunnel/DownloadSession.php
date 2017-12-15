<?php 
namespace ODPS\Services\Tunnel;

use ODPS\Core\OdpsBase;
use ODPS\Core\ResourceBuilder;
use ODPS\Core\OdpsException;
use ODPS\Core\OdpsClient;
use ODPS\Services\Tunnel\Protobuf\TableRecords;

class DownloadSession extends \ODPS\Core\OdpsBase {

    private $sessionJsonObj;
    private $tableName;
    private $partition;

    public function __construct($odpsClient, $partition, $sessionJsonObj, $tableName, $tunnelServer) {
        parent::__construct($odpsClient);
        $this->sessionJsonObj = $sessionJsonObj;
        $this->tableName = $tableName;
        $this->partition = $partition;
        $this->setBaseUrl($tunnelServer);
    }

    /**
     * 获取下载状态
     * */
    public function getStatus() {
        $resource = ResourceBuilder::buildTableUrl($this->getDefaultProjectName(), $this->tableName);
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_PARAMS => array(
                "partition" => $this->partition,
                "downloadid" => $this->sessionJsonObj->DownloadID)
            );

        return $this->call($options);
    }

    /**
     * 获取记录条数
     * @return 记录条数
     * */
    public function getRecordCount() {
        return $this->sessionJsonObj->RecordCount;
    }

    /**
     * 打开一个下载器 
     * @return $recordReader 下载器
     * 简单用法如下
     * while ($record = $recordReader->read()) {
     *     foreach ($record->getValues() as $v) {
     *         print $v;
     *         print ", ";
     *     }
     *     print "\n";
     * }
     * */
    public function openRecordReader($start, $count, $columns = null) {
        $resource = ResourceBuilder::buildTableUrl($this->getDefaultProjectName(), $this->tableName);
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_SUB_RESOURCE => "data",
            OdpsClient::ODPS_PARAMS => array(
                "partition" => $this->partition,
                "downloadid" => $this->sessionJsonObj->DownloadID,
                "columns" => $columns,
                "rowrange" => "(" . $start . "," . $count . ")"
            ),
            OdpsClient::ODPS_HEADERS => array(
                "x-odps-tunnel-version" => constant("XOdpsTunnelVersion"),
                "Accept" => "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"
            ),
            "JustOpenSocket" => true
        );
        $this->setCurrProject($this->getCurrProject());
        return new RecordReader($this->call($options), $this->getColumns());
    }

    /**
     * 获取表列情况 包括名称 类型 和 注释
     * */
    public function getColumns() {
        if ($this->sessionJsonObj == null) {
            throw new OdpsException("Invalid upload session");
        }
        $cols = $this->sessionJsonObj->Schema->columns;
        return $cols;
    }
}
