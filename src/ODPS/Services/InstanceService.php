<?php

namespace ODPS\Services;

use ODPS\Core\OdpsBase;
use ODPS\Core\ResourceBuilder;
use ODPS\Core\OdpsClient;

/**
 * Class Instance provides all operations for odps Instance
 *
 * @package ODPS\Services
 */
class InstanceService extends \ODPS\Core\OdpsBase
{
    /**
     *  查看 instance 状态
     *  @param $instancename    请求的 instance 名称
     *  @return $obj->getInstance($instancename)->Status [ Suspended、 Running 和 Terminated 三种] 
     * */
    public function getInstance($instancename)
    {
        $resource = ResourceBuilder::buildInstanceUrl($this->getDefaultProjectName(), $instancename);

        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource
        );

        return $this->call($options);
    }

    /**
     * get instance detail 操作用于获取 instance 详细信息。
     * @param $instancname    instance 的名称
     * @param $taskname       指定查询的Taskname; 必选参数，如果未输入，返回400错误
     * @url http://repo.aliyun.com/api-doc/Instance/get_instance_detail/index.html
     * */
    public function getInstanceDetail($instancename, $taskName) {
        return $this->_getInstanceAction($instancename, $taskName, "instancedetail");
    }

    /**
     * 目测不好使,可以用 getInstanceTask 代替
     * get instance progress 操作用于获取 instance task 的执行进度。
     * @param $instancname    instance 的名称
     * @param $taskname    指定查询的Taskname; 必选参数，如果未输入，返回400错误
     * @url http://repo.aliyun.com/api-doc/Instance/get_instance_progress/index.html
     * */
    public function getInstanceProgress($instancename, $taskName) {
        return $this->_getInstanceAction($instancename, $taskName, "instanceprogress");
    }

    /**
     * get instance summary 操作用于获取 instance task 的总结信息。
     * @param $instancname    instance 的名称
     * @param $taskname    指定查询的Taskname; 必选参数，如果未输入，返回400错误
     * @url http://repo.aliyun.com/api-doc/Instance/get_instance_summary/index.html
     * */
    public function getInstanceSummary($instancename, $taskName) {
        return $this->_getInstanceAction($instancename, $taskName, "instancesummary");
    }

    /**
     * 根据参数获取全部instances 容器使用，亦可外部使用 
     * @parse 参数列表, 具体见 getInstances
     * */
    public function getInstancesInternal($paras){
        $resource = ResourceBuilder::buildInstancesUrl($this->getDefaultProjectName());
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_PARAMS => $paras
        );
        return $this->call($options);
    }

    /**
     * 查看所有 instance 信息
     * @param $daterange    指定查询instance 开始运行的时间范围。格式为：daterange=[n1]:[n2] ，
     *                          其中n1是指时间范围的开始，n2为结束。n1和n2是将时间日期转换成整型后的数值。
     *                          省略n1，查询条件为截止到n2,格式为:n2；省略n2，查询条件为从n1开始截到现在，格式为 n1:；
     *                          同时省略n1和n2等同于忽略daterange查询条件
     * @param $status       instance的状态，有效值包括：Running,Suspended,Terminated，可省
     * @param $jobname      用于指定通过SubJob task 启动的Job名称，可省
     * @param $onlyowner    指示是否只返回提交查询人自己的instance。有效值包括：yes，no。yes: 只返回自己的，no返回所有的。 参数可省，默认为yes.
     * # @param $marker     指定instance的标识，在返回记录时会从指定标识开始返回记录。
     *                          如果没有指定这个参数，会从第1条返回。
     *                          在API的response中，会返回下一次开始的标识，指定的marker值无效，返回0条记录；
     * # @param maxItems    指定本次查询期待返回最大的记录数，默认为1000。 
     *                          * 如果符合条件的记录小于maxitems，在API 的Response中会返回本次请求中准确的Maxitems； 
     * 
     * @return OdpsIterator object
     * */
    public function getInstances($daterange = null, $status = null, $jobname = null, $onlyowner = null) {
        /**
         * 迭代器遍历
         * $it = new OdpsIterator();
         * $it->rewind();
         * while ($it->valid()){
         *     # $key = $it->key();  无key
         *     $value = $it->current();
         *     $it->next();
         * }
         * */   
        return new OdpsIterator(
            array(
                "obj" => $this,
                "func" => "getInstancesInternal",
                "funcParams" => array(
                    "daterange" => $daterange,
                    "status" => $status,
                    "jobname" => $jobname,
                    "onlyowner" => $onlyowner
                ),
                "itemName" => "Instance"
            )
        );
    }

    /**
     * 查看 instance 中 task 状态
     * @param $instancename 请求的 instance 名称
     * @return $taskStatus = $instance->getInstanceTask($instanceId)->Tasks->Task->Status;
     *   XML: 自行处理 
     *   <?xml version="1.0" encoding="UTF-8"?>
     *       <Instance>
     *           <Status>Terminated</Status>
     *           <Tasks>
     *               <Task Type="SQL">
     *               <Name>@task</Name>
     *               <StartTime>Fri, 15 Dec 2017 14:02:04 GMT</StartTime>
     *               <EndTime>Fri, 15 Dec 2017 14:02:16 GMT</EndTime>
     *               <Status>Success</Status>
     *               <Histories/>
     *               </Task>
     *           </Tasks>
     *       </Instance>
     *   参数        描述
     *   Instance    Instance 描述
     *   Status      Instance 状态
     *   Tasks       Instance 中 task 列表描述
     *   Task        Task 描述
     *   Name        Task 名称
     *   StartTime   Task 开始时间
     *   EndTime     Task 结束时间 
     *   Status      Task 状态
     */
    public function getInstanceTask($instancename) {
        $resource = ResourceBuilder::buildInstanceUrl($this->getDefaultProjectName(), $instancename);
        $options = array(
            OdpsClient::ODPS_METHOD => "GET",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_SUB_RESOURCE => "taskstatus"
        );
        return $this->call($options);
    }

    /**
     * 创建一个SQL任务并返回任务ID(instance name)
     *      ODPS Instance 是用户作业运行的实例，
     *      分为以下几种类型：
     *          SQL, 
     *          SQLPLAN, 
     *          MapReduce, 
     *          DT, 
     *          PLSQL。
     *      用户可以提交不同种类的 Task 来创建对应的 Instance 实例。
     * @param $taskName
     * @param $comment
     * @param $query
     * @return return InstanceName if success,otherwise return original result
     */
    public function postSqlTaskInstance($taskName, $comment, $query) {
        $content = <<<EOT
<?xml version="1.0" ?>
<Instance><Job><Priority>9</Priority><Tasks>
<SQL>
    <Name>$taskName</Name>
    <Comment>$comment</Comment>
    <Config>
        <Property>
            <Name />
            <Value />
        </Property>
    </Config>
    <Query><![CDATA[$query]]></Query>
</SQL>
</Tasks></Job></Instance>
EOT;
        $resource = ResourceBuilder::buildInstancesUrl($this->getDefaultProjectName());
        $options = array(
            OdpsClient::ODPS_METHOD   => "POST",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_CONTENT  => $content,
            OdpsClient::ODPS_CONTENT_TYPE => OdpsClient::XmlContentType
        );
        return $this->_getInstanceName($this->call($options));
    }

    /**
     * 提交一个 ODPS SQL Plan任务，常用与预估 SQL 任务的消耗。
     * ODPS Instance 是用户作业运行的实例，
     *      分为以下几种类型：
     *          SQL, 
     *          SQLPLAN, 
     *          MapReduce, 
     *          DT, 
     *          PLSQL。
     *      用户可以提交不同种类的 Task 来创建对应的 Instance 实例。
     * @param $taskName
     * @param $comment
     * @param $properties
     * @param $query
     * @return return InstanceName if success,otherwise return original result
     */
    public function postSqlPlanTaskInstance($taskName, $comment, $properties, $query) {
        $propertiesXml = "";

        if (is_array($properties) && !empty($properties)) {
            $propertiesXml = "<Config>";
            foreach ($properties as $k => $v) {
                $propertiesXml .= "<Property>";
                $propertiesXml .= "<Name>" . $k . "</Name>";
                $propertiesXml .= "<Value>" . $v . "</Value>";
                $propertiesXml .= "</Property>";
            }
            $propertiesXml .= "</Config>";
        }

        $content = <<<EOT
<?xml version="1.0" ?>
<Instance><Job><Priority>9</Priority><Tasks>
<SQLPlan>
    <Name>$taskName</Name>
    <Comment>$comment</Comment>
    $propertiesXml
    <Query><![CDATA[$query]]></Query>
</SQLPlan>
</Tasks></Job></Instance>
EOT;
        $resource = ResourceBuilder::buildInstancesUrl($this->getDefaultProjectName());
        $options = array(
            OdpsClient::ODPS_METHOD => "POST",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_CONTENT => $content,
            OdpsClient::ODPS_CONTENT_TYPE => OdpsClient::XmlContentType
        );
        return $this->_getInstanceName($this->call($options));
    }

    /**
     * put instance terminated 操作用于中止 instance。 
     * @param $instanceName Instance id
     * */
    public function terminateInstance($instanceName) {
        $resource = ResourceBuilder::buildInstanceUrl($this->getDefaultProjectName(), $instanceName);

        $content = <<<EOT
<?xml version="1.0" ?>
<Instance>
    <Status>Terminated</Status>
</Instance>
EOT;

        $options = array(
            OdpsClient::ODPS_METHOD => "PUT",
            OdpsClient::ODPS_RESOURCE => $resource,
            OdpsClient::ODPS_CONTENT => $content,
            OdpsClient::ODPS_CONTENT_TYPE => OdpsClient::XmlContentType
        );
        return $this->call($options);
    }

    /**
     * 私有工具方法 用于获取 detail/summary/progress
     * @param $instancename Instance id
     * @param $taskName     任务名称
     * @param $action       操作 [instancedetail, instanceprogress, instancesummary ]
     * */
    private function _getInstanceAction($instancename, $taskName, $action) {
        $resource = ResourceBuilder::buildInstanceUrl($this->getDefaultProjectName(), $instancename);
        $options = array(
            OdpsClient::ODPS_METHOD    => "GET",
            OdpsClient::ODPS_RESOURCE  => $resource,
            OdpsClient::ODPS_SUB_RESOURCE => $action,
            OdpsClient::ODPS_PARAMS => array(
                "taskname" => $taskName
            )
        );
        return $this->call($options);
    }

    /**
     * 获取 Instance name
     * @param $rst
     * */
    private function _getInstanceName($rst) {
        if (isset($rst->header["location"])) {
            $location = $rst->header["location"];
            $locationUrl = parse_url($location);
            $pathArr = explode("/", $locationUrl["path"]);
            return $pathArr[count($pathArr) - 1];
        } else {
            return $rst;
        }
    }
}
