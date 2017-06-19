<?php
namespace MysqlDB;

use MysqlDB\Conf\Config;
use MysqlDB\Conf\Common;
use Exception;

defined('DB_FIELDS_CACHE_PATH') or define('DB_FIELDS_CACHE_PATH', __DIR__ . '/Cache');

class Model 
{
    // 操作状态
    const MODEL_INSERT          =   1;       // 插入模型数据
    const MODEL_UPDATE          =   2;       // 更新模型数据
    const MODEL_BOTH            =   3;       // 包含上面两种方式
    const MUST_VALIDATE         =   1;       // 必须验证
    const EXISTS_VALIDATE       =   0;       // 表单存在字段则验证
    const VALUE_VALIDATE        =   2;       // 表单值不为空则验证

    protected $db               =   null;	 // 当前数据库操作对象
    protected $pk               =   'id';	 // 主键名称
    protected $autoinc          =   false;   // 主键是否自动增长 
    protected $tablePrefix      =   null;	 // 数据表前缀
    protected $name             =   '';		 // 模型名称
    protected $dbName           =   '';		 // 数据库名称
    protected $error            =   '';		 // 最近错误信息
    protected $fields           =   array(); // 字段信息
    protected $data             =   array(); // 数据信息
    protected $options          =   array(); // 查询表达式参数
    protected $_validate        =   array();  // 自动验证定义
    protected $_auto            =   array();  // 自动完成定义
    protected $autoCheckFields  =   true;	  // 是否自动检测数据表字段信息
    protected $patchValidate    =   false;    // 是否批处理验证
    protected $methods          =   array('strict', 'order', 'alias', 'having', 'group', 'lock', 'distinct', 'auto', 'filter', 'validate', 'result', 'token', 'index', 'force');

    // 架构函数
    public function __construct($name, $tablePrefix='', array $connection=array()) 
    {
        // 获取模型名称
        if(strpos($name, '.')) {
            list($this->dbName, $this->name) = explode('.', $name);
        } else {
            $this->name   =  $name;
        }

        // 设置表前缀
        if(is_null($tablePrefix)) {
            $this->tablePrefix = '';
        } elseif('' != $tablePrefix) {
            $this->tablePrefix = $tablePrefix;
        } elseif(! isset($this->tablePrefix)) {
            $this->tablePrefix = Config::DB_PREFIX;
        }

        // 数据库初始化操作
        $this->connection($connection, true);
    }
	
	// 连接数据库
	public function connection(array $connection=array(), $force=falase)
	{
		if(! isset($this->db) || $force) 
		{
            $this->db = new Mysql($this->_parseConfig($connection));
        }

        // 字段检测
        if(! empty($this->name)) $this->_checkTableInfo();
        return $this;
	}
	
	// 数据库连接参数解析
    private function _parseConfig(array $config)
	{
		if(! empty($config)) {
            $config =   array_change_key_case($config);
            $config = array (
                'username'      =>  $config['db_user'] ? $config['db_user'] : Config::DB_USER,
                'password'      =>  $config['db_pwd'] ? $config['db_pwd'] : Config::DB_PWD,
                'hostname'      =>  $config['db_host'] ? $config['db_host'] : Config::DB_HOST,
                'hostport'      =>  $config['db_port'] ? $config['db_port'] : Config::DB_PORT,
                'database'      =>  $config['db_name'] ? $config['db_name'] : Config::DB_NAME,
                'charset'       =>  isset($config['db_charset'])?$config['db_charset'] : Config::DB_CHARSET
            );
        } else {
            $config = array (
                'username'      =>  Config::DB_USER,
                'password'      =>  Config::DB_PWD,
                'hostname'      =>  Config::DB_HOST,
                'hostport'      =>  Config::DB_PORT,
                'database'      =>  Config::DB_NAME,
                'charset'       =>  Config::DB_CHARSET
            );
        }
        return $config;
    }
	
    // 自动检测数据表信息
    protected function _checkTableInfo() 
	{
        if(empty($this->fields)) 
		{
            $db   =  $this->dbName ? $this->dbName : Config::DB_NAME;
            $path = DB_FIELDS_CACHE_PATH . '/_fields/' . strtolower($db . '.' . $this->tablePrefix . $this->name) . '.php';
            if(Config::DB_FIELDS_CACHE && file_exists($path)) 
			{
				$fields = unserialize(file_get_contents($path));
                if($fields && is_array($fields)) 
				{
                    $this->fields = $fields;
                    if(! empty($fields['_pk'])) 
					{
                        $this->pk = $fields['_pk'];
                    }
                    return ;
                }
            }
            $this->flush();
        }
    }

    // 获取字段信息并缓存
    public function flush()
	{
        // 缓存不存在则查询数据表信息
        $this->db->setModel($this->name);
        $fields =   $this->db->getFields($this->getTableName());
        if(! $fields) return false;
        $this->fields = array_keys($fields);
        unset($this->fields['_pk']);
		
		// 记录字段类型
		$type = array();
        foreach ($fields as $key=>$val) {
            $type[$key]     =   $val['type'];
            if($val['primary']) {
                if (isset($this->fields['_pk']) && $this->fields['_pk'] != null) {
                    if (is_string($this->fields['_pk'])) {
                        $this->pk = array($this->fields['_pk']);
                        $this->fields['_pk']   =   $this->pk;
                    }
                    $this->pk[]   =   $key;
                    $this->fields['_pk'][]   =   $key;
                } else {
                    $this->pk   =   $key;
                    $this->fields['_pk']   =   $key;
                }
                if($val['autoinc']) $this->autoinc   =   true;
            }
        }
		
        // 记录字段类型信息
        $this->fields['_type'] =  $type;

        if(Config::DB_FIELDS_CACHE) 
		{
			$db = $this->dbName ? $this->dbName : Config::DB_NAME;
            $path = DB_FIELDS_CACHE_PATH . '/_fields/';
            if(! file_exists($path)) mkdir($path, 0755, true);

			file_put_contents($path . strtolower($db . '.' . $this->tablePrefix . $this->name) . '.php', serialize($this->fields));
        }
    }

    public function __set($name, $value) 
	{
        $this->data[$name]  =   $value;
    }

    public function __get($name) 
	{
        return isset($this->data[$name])?$this->data[$name]:null;
    }

    public function __isset($name) 
	{
        return isset($this->data[$name]);
    }

    public function __unset($name) 
	{
        unset($this->data[$name]);
    }

    // 利用__call方法实现一些特殊的Model方法
    public function __call($method,$args) 
	{
        if(in_array(strtolower($method),$this->methods,true)) {
            // 连贯操作的实现
            $this->options[strtolower($method)] =   $args[0];
            return $this;
        } elseif(in_array(strtolower($method), array('count','sum','min','max','avg'), true)) {
            // 统计查询的实现
            $field =  isset($args[0])?$args[0]:'*';
            return $this->getField(strtoupper($method).'('.$field.') AS tp_'.$method);
        } elseif(strtolower(substr($method,0,5))=='getby') {
            // 根据某个字段获取记录
            $field   =   Common::parseName(substr($method,5));
            $where[$field] =  $args[0];
            return $this->where($where)->find();
        } elseif(strtolower(substr($method,0,10))=='getfieldby') {
            // 根据某个字段获取记录的某个值
            $name   =   Common::parseName(substr($method,10));
            $where[$name] =$args[0];
            return $this->where($where)->getField($args[1]);
        } else {
			return new \Exception(__CLASS__ . ': ' . $method . ' 不存在');
            return;
        }
    }

    // 对保存到数据库的数据进行处理
    protected function _facade($data) 
	{
        // 检查数据字段合法性
        if(! empty($this->fields)) {
            if(! empty($this->options['field'])) {
                $fields =   $this->options['field'];
                unset($this->options['field']);
                if(is_string($fields)) {
                    $fields =   explode(',',$fields);
                }    
            } else {
                $fields =   $this->fields;
            }        
            foreach ($data as $key=>$val) {
                if(! in_array($key, $fields, true)) {
                    if(! empty($this->options['strict'])) {
						return new Exception('非法数据对象' . ':[' . $key . '=>' . $val . ']');
                    }
                    unset($data[$key]);
                } elseif(is_scalar($val)) {
                    $this->_parseType($data, $key);
                }
            }
        }
       
        // 安全过滤
        if(! empty($this->options['filter'])) {
            $data = array_map($this->options['filter'], $data);
            unset($this->options['filter']);
        }
        return $data;
     }

    // 新增数据
    public function add($data='', $options=array(), $replace=false) 
	{
		// 没有传递数据，获取当前数据对象的值
        if(empty($data)) {
            if(!empty($this->data)) {
                $data           =   $this->data;
                $this->data     = array();
            }else{
                $this->error    = "非法数据对象";
                return false;
            }
        }
		
        // 数据处理
        $data       =   $this->_facade($data);
        // 分析表达式
        $options    =   $this->_parseOptions($options);
        // 写入数据到数据库
        $result = $this->db->insert($data,$options,$replace);
        if(false !== $result && is_numeric($result)) {
            $pk     =   $this->getPk();
            if (is_array($pk)) return $result;
            $insertId   =   $this->getLastInsID();
            if($insertId) {
                $data[$pk]  = $insertId;
                return $insertId;
            }
        }
        return $result;
    }

    public function addAll($dataList,$options=array(),$replace=false)
	{
        if(empty($dataList)) {
            $this->error = '非法数据对象';
            return false;
        }
		
        // 数据处理
        foreach ($dataList as $key=>$data) {
            $dataList[$key] = $this->_facade($data);
        }
        // 分析表达式
        $options =  $this->_parseOptions($options);
        
		// 写入数据到数据库
        $result = $this->db->insertAll($dataList, $options, $replace);
        if(false !== $result ) 
		{
            $insertId   =   $this->getLastInsID();
            if($insertId) {
                return $insertId;
            }
        }
        return $result;
    }

    // 保存数据
    public function save($data='',$options=array()) 
	{
        if(empty($data)) {
            if(!empty($this->data)) {
                $data           =   $this->data;
                $this->data     =   array();
            } else {
                $this->error    =   "非法数据对象";
                return false;
            }
        }
		
        // 数据处理
        $data       =   $this->_facade($data);
        if(empty($data)) {
            $this->error    =   "非法数据对象";
            return false;
        }
		
        // 分析表达式
        $options    =   $this->_parseOptions($options);
        $pk         =   $this->getPk();
        if(!isset($options['where']) ) {
            // 如果存在主键数据 则自动作为更新条件
            if (is_string($pk) && isset($data[$pk])) {
                $where[$pk]     =   $data[$pk];
                unset($data[$pk]);
            } elseif (is_array($pk)) {
                // 增加复合主键支持
                foreach ($pk as $field) {
                    if(isset($data[$field])) {
                        $where[$field]      =   $data[$field];
                    } else {
                        $this->error        =   "操作出现错误";
                        return false;
                    }
                    unset($data[$field]);
                }
            }
            if(! isset($where)) {
                $this->error        =   "操作出现错误";
                return false;
            } else {
                $options['where']   =   $where;
            }
        }

        if(is_array($options['where']) && isset($options['where'][$pk])) {
            $pkValue    =   $options['where'][$pk];
        }
		
        $result     =   $this->db->update($data,$options);
        if(false !== $result && is_numeric($result)) {
            if(isset($pkValue)) $data[$pk]   =  $pkValue;
        }
        return $result;
    }

    // 删除数据
    public function delete($options=array()) 
	{
        $pk   =  $this->getPk();
        if(empty($options) && empty($this->options['where'])) {
            // 如果删除条件为空 则删除当前数据对象所对应的记录
            if(!empty($this->data) && isset($this->data[$pk]))
                return $this->delete($this->data[$pk]);
            else
                return false;
        }
        if(is_numeric($options)  || is_string($options)) {
            // 根据主键删除记录
            if(strpos($options,',')) {
                $where[$pk]     =  array('IN', $options);
            }else{
                $where[$pk]     =  $options;
            }
            $options            =  array();
            $options['where']   =  $where;
        }
        // 根据复合主键删除记录
        if (is_array($options) && (count($options) > 0) && is_array($pk)) {
            $count = 0;
            foreach (array_keys($options) as $key) {
                if (is_int($key)) $count++; 
            } 
            if ($count == count($pk)) {
                $i = 0;
                foreach ($pk as $field) {
                    $where[$field] = $options[$i];
                    unset($options[$i++]);
                }
                $options['where']  =  $where;
            } else {
                return false;
            }
        }
        // 分析表达式
        $options =  $this->_parseOptions($options);
        if(empty($options['where'])){
            // 如果条件为空 不进行删除操作 除非设置 1=1
            return false;
        }        
        if(is_array($options['where']) && isset($options['where'][$pk])){
            $pkValue            =  $options['where'][$pk];
        }

        $result  =    $this->db->delete($options);
        if(false !== $result && is_numeric($result)) {
            $data = array();
            if(isset($pkValue)) $data[$pk]   =  $pkValue;
        }
        // 返回删除记录个数
        return $result;
    }

    // 查询数据集
    public function select($options=array()) 
	{
        $pk   =  $this->getPk();
        if(is_string($options) || is_numeric($options)) {
            // 根据主键查询
            if(strpos($options,',')) {
                $where[$pk]     =  array('IN',$options);
            }else{
                $where[$pk]     =  $options;
            }
            $options            =  array();
            $options['where']   =  $where;
        }elseif (is_array($options) && (count($options) > 0) && is_array($pk)) {
            // 根据复合主键查询
            $count = 0;
            foreach (array_keys($options) as $key) {
                if (is_int($key)) $count++; 
            } 
            if ($count == count($pk)) {
                $i = 0;
                foreach ($pk as $field) {
                    $where[$field] = $options[$i];
                    unset($options[$i++]);
                }
                $options['where']  =  $where;
            } else {
                return false;
            }
        } elseif(false === $options){ // 用于子查询 不查询只返回SQL
            $options['fetch_sql'] = true;
        }
        // 分析表达式
        $options    =  $this->_parseOptions($options);
        $resultSet  = $this->db->select($options);
        if(false === $resultSet) {
            return false;
        }
        if(!empty($resultSet)) { // 有查询结果
            if(is_string($resultSet)){
                return $resultSet;
            }

            if(isset($options['index'])){ // 对数据集进行索引
                $index  =   explode(',',$options['index']);
                foreach ($resultSet as $result){
                    $_key   =  $result[$index[0]];
                    if(isset($index[1]) && isset($result[$index[1]])){
                        $cols[$_key] =  $result[$index[1]];
                    }else{
                        $cols[$_key] =  $result;
                    }
                }
                $resultSet  =   $cols;
            }
        }
        return $resultSet;
    }

    // 生成查询SQL 可用于子查询
    public function buildSql() 
	{
        return  '( '.$this->fetchSql(true)->select().' )';
    }

    // 分析表达式
    protected function _parseOptions($options=array()) 
	{
        if(is_array($options))
            $options =  array_merge($this->options, $options);

        if(! isset($options['table'])) {
            $options['table']   =   $this->getTableName();
            $fields             =   $this->fields;
        } else {
            $fields             =   $this->getDbFields();
        }

        // 数据表别名
        if(! empty($options['alias'])) {
            $options['table']  .=   ' '.$options['alias'];
        }
		
        // 记录操作的模型名称
        $options['model']       =   $this->name;

        // 字段类型验证
        if(isset($options['where']) && is_array($options['where']) && !empty($fields) && !isset($options['join'])) {
            // 对数组查询条件进行字段类型检查
            foreach ($options['where'] as $key=>$val) {
                $key            =   trim($key);
                if(in_array($key, $fields, true)) {
                    if(is_scalar($val)) {
                        $this->_parseType($options['where'],$key);
                    }
                } elseif(!is_numeric($key) && '_' != substr($key,0,1) && false === strpos($key,'.') && false === strpos($key,'(') && false === strpos($key,'|') && false === strpos($key,'&')) {
                    if(! empty($this->options['strict'])) {
						return new Exception('错误的查询条件' . ':[' . $key . '=>' . $val . ']');
                    } 
                    unset($options['where'][$key]);
                }
            }
        }
		
        // 查询过后清空sql表达式组装 避免影响下次查询
        $this->options  =   array();
        return $options;
    }

    // 数据类型检测
    protected function _parseType(&$data,$key) 
	{
        if(! isset($this->options['bind'][':'.$key]) && isset($this->fields['_type'][$key])) {
            $fieldType = strtolower($this->fields['_type'][$key]);
            if(false !== strpos($fieldType, 'enum')) {
                // 支持ENUM类型优先检测
            } elseif(false === strpos($fieldType,'bigint') && false !== strpos($fieldType,'int')) {
                $data[$key]   =  intval($data[$key]);
            } elseif(false !== strpos($fieldType,'float') || false !== strpos($fieldType,'double')) {
                $data[$key]   =  floatval($data[$key]);
            } elseif(false !== strpos($fieldType,'bool')) {
                $data[$key]   =  (bool)$data[$key];
            }
        }
    }

    // 查询数据
    public function find($options=array()) 
	{
        if(is_numeric($options) || is_string($options)) {
            $where[$this->getPk()]  =   $options;
            $options                =   array();
            $options['where']       =   $where;
        }
        // 根据复合主键查找记录
        $pk  =  $this->getPk();
        if (is_array($options) && (count($options) > 0) && is_array($pk)) {
            // 根据复合主键查询
            $count = 0;
            foreach (array_keys($options) as $key) {
                if (is_int($key)) $count++; 
            } 
            if ($count == count($pk)) {
                $i = 0;
                foreach ($pk as $field) {
                    $where[$field] = $options[$i];
                    unset($options[$i++]);
                }
                $options['where']  =  $where;
            } else {
                return false;
            }
        }
        // 总是查找一条记录
        $options['limit']   =   1;
        // 分析表达式
        $options            =   $this->_parseOptions($options);
        $resultSet          =   $this->db->select($options);
        if(false === $resultSet) {
            return false;
        }
        if(empty($resultSet)) {// 查询结果为空
            return null;
        }
        if(is_string($resultSet)){
            return $resultSet;
        }

        // 读取数据后的处理
        $data   =   $resultSet[0];
        if(! empty($this->options['result'])) {
            return $this->returnResult($data, $this->options['result']);
        }
        $this->data     =   $data;
        return $this->data;
    }

    protected function returnResult($data,$type=''){
        if ($type){
            if(is_callable($type)){
                return call_user_func($type,$data);
            }
            switch (strtolower($type)){
                case 'json':
                    return json_encode($data);
                case 'xml':
                    return xml_encode($data);
            }
        }
        return $data;
    }

    // 设置记录的某个字段值
    public function setField($field,$value='') 
    {
        if(is_array($field)) {
            $data           = $field;
        } else {
            $data[$field]   = $value;
        }
        return $this->save($data);
    }

    // 字段值增长
    public function setInc($field, $step=1) 
	{
        return $this->setField($field,array('exp',$field.'+'.$step));
    }

    // 字段值减少
    public function setDec($field, $step=1) 
	{
        return $this->setField($field, array('exp', $field.'-'.$step));
    }

    // 获取一条记录的某个字段值
    public function getField($field,$sepa=null) 
	{
        $options['field']       =   $field;
        $options                =   $this->_parseOptions($options);
        $field                  =   trim($field);
        if(strpos($field,',') && false !== $sepa) { // 多字段
            if(!isset($options['limit'])){
                $options['limit']   =   is_numeric($sepa)?$sepa:'';
            }
            $resultSet          =   $this->db->select($options);
            if(!empty($resultSet)) {
                if(is_string($resultSet)){
                    return $resultSet;
                }               
                $_field         =   explode(',', $field);
                $field          =   array_keys($resultSet[0]);
                $key1           =   array_shift($field);
                $key2           =   array_shift($field);
                $cols           =   array();
                $count          =   count($_field);
                foreach ($resultSet as $result){
                    $name   =  $result[$key1];
                    if(2==$count) {
                        $cols[$name]   =  $result[$key2];
                    }else{
                        $cols[$name]   =  is_string($sepa)?implode($sepa,array_slice($result,1)):$result;
                    }
                }
                return $cols;
            }
        } else {   // 查找一条记录
            // 返回数据个数
            if(true !== $sepa) {// 当sepa指定为true的时候 返回所有数据
                $options['limit']   =   is_numeric($sepa)?$sepa:1;
            }
            $result = $this->db->select($options);
            if(!empty($result)) {
                if(is_string($result)){
                    return $result;
                }               
                if(true !== $sepa && 1==$options['limit']) {
                    $data   =   reset($result[0]);
                    return $data;
                }
                foreach ($result as $val){
                    $array[]    =   $val[$field];
                }
                return $array;
            }
        }
        return null;
    }

    // 创建数据对象 但不保存到数据库
    public function create($data='',$type='') 
    {
        if(empty($data)) {
            $data   =   $_POST;
        } elseif(is_object($data)) {
            $data   =   get_object_vars($data);
        }

        // 验证数据
        if(empty($data) || !is_array($data)) 
        {
            $this->error = '非法数据对象';
            return false;
        }

        // 状态
        $type = $type?:(!empty($data[$this->getPk()])?self::MODEL_UPDATE:self::MODEL_INSERT);

        // 检测提交字段的合法性
        if(isset($this->options['field'])) { // $this->field('field1,field2...')->create()
            $fields =   $this->options['field'];
            unset($this->options['field']);
        } elseif($type == self::MODEL_INSERT && isset($this->insertFields)) {
            $fields =   $this->insertFields;
        } elseif($type == self::MODEL_UPDATE && isset($this->updateFields)) {
            $fields =   $this->updateFields;
        }
		
        if(isset($fields)) {
            if(is_string($fields)) {
                $fields =   explode(',', $fields);
            }
            foreach ($data as $key=>$val) {
                if(!in_array($key, $fields)) {
                    unset($data[$key]);
                }
            }
        }

        // 数据自动验证
        if(!$this->autoValidation($data,$type)) return false;

        // 验证完成生成数据对象
        if($this->autoCheckFields) { // 开启字段检测 则过滤非法字段数据
            $fields =   $this->getDbFields();
            foreach ($data as $key=>$val){
                if(!in_array($key,$fields)) {
                    unset($data[$key]);
                }elseif(MAGIC_QUOTES_GPC && is_string($val)){
                    $data[$key] =   stripslashes($val);
                }
            }
        }

        // 创建完成对数据进行自动处理
        $this->autoOperation($data, $type);
        $this->data =   $data;
        return $data;
     }

    // 使用正则验证数据
    public function regex($value,$rule) {
        $validate = array(
            'require'   =>  '/\S+/',
            'email'     =>  '/^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$/',
            'url'       =>  '/^http(s?):\/\/(?:[A-za-z0-9-]+\.)+[A-za-z]{2,4}(:\d+)?(?:[\/\?#][\/=\?%\-&~`@[\]\':+!\.#\w]*)?$/',
            'currency'  =>  '/^\d+(\.\d+)?$/',
            'number'    =>  '/^\d+$/',
            'zip'       =>  '/^\d{6}$/',
            'integer'   =>  '/^[-\+]?\d+$/',
            'double'    =>  '/^[-\+]?\d+(\.\d+)?$/',
            'english'   =>  '/^[A-Za-z]+$/',
        );
        // 检查是否有内置的正则表达式
        if(isset($validate[strtolower($rule)]))
            $rule       =   $validate[strtolower($rule)];
        return preg_match($rule,$value)===1;
    }

    // 自动表单处理
    private function autoOperation(&$data,$type) {
        if(false === $this->options['auto']){
            // 关闭自动完成
            return $data;
        }
        if(!empty($this->options['auto'])) {
            $_auto   =   $this->options['auto'];
            unset($this->options['auto']);
        }elseif(!empty($this->_auto)){
            $_auto   =   $this->_auto;
        }
        // 自动填充
        if(isset($_auto)) {
            foreach ($_auto as $auto)
            {
                // 填充因子定义格式
                if(empty($auto[2])) $auto[2] =  self::MODEL_INSERT; // 默认为新增的时候自动填充
                if( $type == $auto[2] || $auto[2] == self::MODEL_BOTH) {
                    if(empty($auto[3])) $auto[3] =  'string';
                    switch(trim($auto[3])) {
                        case 'function':    //  使用函数进行填充 字段的值作为参数
                        case 'callback': // 使用回调方法
                            $args = isset($auto[4])?(array)$auto[4]:array();
                            if(isset($data[$auto[0]])) {
                                array_unshift($args,$data[$auto[0]]);
                            }
                            if('function'==$auto[3]) {
                                $data[$auto[0]]  = call_user_func_array($auto[1], $args);
                            }else{
                                $data[$auto[0]]  =  call_user_func_array(array(&$this,$auto[1]), $args);
                            }
                            break;
                        case 'field':    // 用其它字段的值进行填充
                            $data[$auto[0]] = $data[$auto[1]];
                            break;
                        case 'ignore': // 为空忽略
                            if($auto[1]===$data[$auto[0]])
                                unset($data[$auto[0]]);
                            break;
                        case 'string':
                        default: // 默认作为字符串填充
                            $data[$auto[0]] = $auto[1];
                    }
                    if(isset($data[$auto[0]]) && false === $data[$auto[0]] )   unset($data[$auto[0]]);
                }
            }
        }
        return $data;
    }

    // 自动表单验证
    protected function autoValidation($data,$type) {
        if(false === $this->options['validate'] ){
            // 关闭自动验证
            return true;
        }
        if(!empty($this->options['validate'])) {
            $_validate   =   $this->options['validate'];
            unset($this->options['validate']);
        }elseif(!empty($this->_validate)){
            $_validate   =   $this->_validate;
        }
        // 属性验证
        if(isset($_validate)) { // 如果设置了数据自动验证则进行数据验证
            if($this->patchValidate) { // 重置验证错误信息
                $this->error = array();
            }
            foreach($_validate as $key=>$val) {
                // 判断是否需要执行验证
                if(empty($val[5]) || ( $val[5]== self::MODEL_BOTH && $type < 3 ) || $val[5]== $type ) {
                    if(0==strpos($val[2],'{%') && strpos($val[2],'}'))
                        // 支持提示信息的多语言 使用 {%语言定义} 方式
                        $val[2]  =  L(substr($val[2],2,-1));
                    $val[3]  =  isset($val[3])?$val[3]:self::EXISTS_VALIDATE;
                    $val[4]  =  isset($val[4])?$val[4]:'regex';
                    // 判断验证条件
                    switch($val[3]) {
                        case self::MUST_VALIDATE:   // 必须验证 不管表单是否有设置该字段
                            if(false === $this->_validationField($data,$val)) 
                                return false;
                            break;
                        case self::VALUE_VALIDATE:    // 值不为空的时候才验证
                            if('' != trim($data[$val[0]]))
                                if(false === $this->_validationField($data,$val)) 
                                    return false;
                            break;
                        default:    // 默认表单存在该字段就验证
                            if(isset($data[$val[0]]))
                                if(false === $this->_validationField($data,$val)) 
                                    return false;
                    }
                }
            }
            // 批量验证的时候最后返回错误
            if(!empty($this->error)) return false;
        }
        return true;
    }

    // 验证表单字段 支持批量验证
    protected function _validationField($data,$val) {
        if($this->patchValidate && isset($this->error[$val[0]]))
            return ; //当前字段已经有规则验证没有通过
        if(false === $this->_validationFieldItem($data,$val)){
            if($this->patchValidate) {
                $this->error[$val[0]]   =   $val[2];
            }else{
                $this->error            =   $val[2];
                return false;
            }
        }
        return ;
    }

    // 根据验证因子验证字段
    protected function _validationFieldItem($data,$val) {
        switch(strtolower(trim($val[4]))) {
            case 'function':// 使用函数进行验证
            case 'callback':// 调用方法进行验证
                $args = isset($val[6])?(array)$val[6]:array();
                if(is_string($val[0]) && strpos($val[0], ','))
                    $val[0] = explode(',', $val[0]);
                if(is_array($val[0])){
                    // 支持多个字段验证
                    foreach($val[0] as $field)
                        $_data[$field] = $data[$field];
                    array_unshift($args, $_data);
                }else{
                    array_unshift($args, $data[$val[0]]);
                }
                if('function'==$val[4]) {
                    return call_user_func_array($val[1], $args);
                }else{
                    return call_user_func_array(array(&$this, $val[1]), $args);
                }
            case 'confirm': // 验证两个字段是否相同
                return $data[$val[0]] == $data[$val[1]];
            case 'unique': // 验证某个值是否唯一
                if(is_string($val[0]) && strpos($val[0],','))
                    $val[0]  =  explode(',',$val[0]);
                $map = array();
                if(is_array($val[0])) {
                    // 支持多个字段验证
                    foreach ($val[0] as $field)
                        $map[$field]   =  $data[$field];
                }else{
                    $map[$val[0]] = $data[$val[0]];
                }
                $pk =   $this->getPk();
                if(!empty($data[$pk]) && is_string($pk)) { // 完善编辑的时候验证唯一
                    $map[$pk] = array('neq',$data[$pk]);
                }
                if($this->where($map)->find())   return false;
                return true;
            default:  // 检查附加规则
                return $this->check($data[$val[0]],$val[1],$val[4]);
        }
    }

    // 验证数据 支持 in between equal length regex expire ip_allow ip_deny
    public function check($value,$rule,$type='regex')
	{
        $type   =   strtolower(trim($type));
        switch($type) {
            case 'in':
            case 'notin':
                $range   = is_array($rule)? $rule : explode(',',$rule);
                return $type == 'in' ? in_array($value ,$range) : !in_array($value ,$range);
            case 'between': // 验证是否在某个范围
            case 'notbetween': // 验证是否不在某个范围            
                if (is_array($rule)){
                    $min    =    $rule[0];
                    $max    =    $rule[1];
                }else{
                    list($min,$max)   =  explode(',',$rule);
                }
                return $type == 'between' ? $value>=$min && $value<=$max : $value<$min || $value>$max;
            case 'equal': // 验证是否等于某个值
            case 'notequal': // 验证是否等于某个值            
                return $type == 'equal' ? $value == $rule : $value != $rule;
            case 'length': // 验证长度
                $length  =  mb_strlen($value,'utf-8'); // 当前数据长度
                if(strpos($rule,',')) { // 长度区间
                    list($min,$max)   =  explode(',',$rule);
                    return $length >= $min && $length <= $max;
                }else{// 指定长度
                    return $length == $rule;
                }
            case 'expire':
                list($start,$end)   =  explode(',',$rule);
                if(!is_numeric($start)) $start   =  strtotime($start);
                if(!is_numeric($end)) $end   =  strtotime($end);
                return time() >= $start && time() <= $end;
            case 'regex':
            default:
                return $this->regex($value,$rule);
        }
    }

    // 存储过程返回多数据集
    public function procedure($sql, $parse = false) 
	{
        return $this->db->procedure($sql, $parse);
    }

    // SQL查询
    public function query($sql,$parse=false) 
	{
        if(!is_bool($parse) && !is_array($parse)) {
            $parse = func_get_args();
            array_shift($parse);
        }
        $sql  =   $this->parseSql($sql,$parse);
        return $this->db->query($sql);
    }

    // 执行SQL语句
    public function execute($sql,$parse=false) 
	{
        if(!is_bool($parse) && !is_array($parse)) {
            $parse = func_get_args();
            array_shift($parse);
        }
        $sql  =   $this->parseSql($sql,$parse);
        return $this->db->execute($sql);
    }

    // 解析SQL语句
    protected function parseSql($sql,$parse) 
	{
        // 分析表达式
        if(true === $parse) {
            $options =  $this->_parseOptions();
            $sql     =   $this->db->parseSql($sql,$options);
        } elseif(is_array($parse)) { // SQL预处理
            $parse   =   array_map(array($this->db,'escapeString'),$parse);
            $sql     =   vsprintf($sql,$parse);
        } else {
            $sql     =   strtr($sql,array('__TABLE__'=>$this->getTableName(),'__PREFIX__'=>$this->tablePrefix));
            $prefix  =   $this->tablePrefix;
            $sql     =   preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function($match) use($prefix){ return $prefix.strtolower($match[1]);}, $sql);
        }
        $this->db->setModel($this->name);
        return $sql;
    }

    // 连接数据库
    public function db(array $config=array(), $force=false) 
	{
        if(! isset($this->db) || $force) {
            $this->db = Db::getInstance($config);
        } elseif(NULL === $config) {
            $this->db->close();
            unset($this->_db);
			return ;
        }

        // 字段检测
        if(! empty($this->name) && $this->autoCheckFields) $this->_checkTableInfo();
        return $this;
    }

    // 得到完整的数据表名
    public function getTableName() 
	{
		$tableName  = ! empty($this->tablePrefix) ? $this->tablePrefix : '';
		$tableName .= Common::parseName($this->name);
        return (! empty($this->dbName) ? $this->dbName . '.' : '') . strtolower($tableName);
    }

    // 启动事务
    public function startTrans() {
        $this->commit();
        $this->db->startTrans();
        return $this;
    }

    // 提交事务
    public function commit() {
        return $this->db->commit();
    }

    // 事务回滚
    public function rollback() 
    {
        return $this->db->rollback();
    }

    // 返回错误信息
    public function getError()
	{
        return $this->db->getError();
    }

    // 返回最后插入的ID
    public function getLastInsID() 
	{
        return $this->db->getLastInsID();
    }

    // 返回最后执行的sql语句
    public function getLastSql() 
	{
        return $this->db->getLastSql($this->name);
    }
	
    // 获取主键名称
    public function getPk() 
	{
        return $this->pk;
    }

    // 获取数据表字段信息
    public function getDbFields()
    {
        if(isset($this->options['table'])) 
        {
            if(is_array($this->options['table'])) {
                $table = key($this->options['table']);
            } else {
                $table = $this->options['table'];
                if(strpos($table,')')) {
                    return false;
                }
            }
            $fields = $this->db->getFields($table);
            return  $fields ? array_keys($fields) : false;
        }
        if($this->fields) {
            $fields     =  $this->fields;
            unset($fields['_type'],$fields['_pk']);
            return $fields;
        }
        return false;
    }

    // 设置数据对象值
    public function data($data = '')
	{
        if('' === $data && !empty($this->data)) {
            return $this->data;
        }
        if(is_object($data)) {
            $data   =   get_object_vars($data);
        } elseif(is_string($data)) {
            parse_str($data, $data);
        } elseif(!is_array($data)) {
            return new Exception('非法数据对象');
        }
        $this->data = $data;
        return $this;
    }

    // 指定当前的数据表
    public function table($table) 
	{
        $prefix =   $this->tablePrefix;
        if(is_array($table)) {
            $this->options['table'] =   $table;
        } elseif(! empty($table)) {
            $table  = preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function($match) use($prefix){ return $prefix.strtolower($match[1]);}, $table);
            $this->options['table'] =   $table;
        }
        return $this;
    }

    // 查询SQL组装 join
    public function join($join, $type='INNER') 
	{
        $prefix = $this->tablePrefix;
        if(is_array($join)) {
            foreach ($join as $key=>&$_join){
                $_join = preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function($match) use($prefix){ return $prefix.strtolower($match[1]);}, $_join);
                $_join = false !== stripos($_join,'JOIN')? $_join : $type.' JOIN ' .$_join;
            }
            $this->options['join']      =   $join;
        } elseif(!empty($join)) {
            $join  = preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function($match) use($prefix){ return $prefix.strtolower($match[1]);}, $join);
            $this->options['join'][]    =   false !== stripos($join,'JOIN')? $join : $type.' JOIN '.$join;
        }
        return $this;
    }

    // 查询SQL组装 union
    public function union($union,$all=false) 
    {
        if(empty($union)) return $this;
        if($all) {
            $this->options['union']['_all']  =   true;
        }
        if(is_object($union)) {
            $union   =  get_object_vars($union);
        }

        // 转换union表达式
        if(is_string($union)) {
            $prefix =   $this->tablePrefix;
            $options  = preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function($match) use($prefix) { return $prefix.strtolower($match[1]);}, $union);
        } elseif(is_array($union)) {
            if(isset($union[0])) {
                $this->options['union'] = array_merge($this->options['union'],$union);
                return $this;
            } else {
                $options =  $union;
            }
        } else {
            return new Exception('非法数据对象');
        }
        $this->options['union'][]  =   $options;
        return $this;
    }

    // 指定查询字段 支持字段排除
    public function field($field, $except=false)
    {
        if(true === $field) {
            $fields     =  $this->getDbFields();
            $field      =  $fields ?: '*';
        } elseif($except) {
            if(is_string($field)) {
                $field  =  explode(',', $field);
            }
            $fields     =  $this->getDbFields();
            $field      =  $fields?array_diff($fields,$field):$field;
        }
        $this->options['field']   =   $field;
        return $this;
    }

    // 指定查询条件 支持安全过滤
    public function where($where, $parse=null)
	{
        if(! is_null($parse) && is_string($where)) {
            if(!is_array($parse)) {
                $parse = func_get_args();
                array_shift($parse);
            }
            $parse = array_map(array($this->db,'escapeString'),$parse);
            $where =   vsprintf($where,$parse);
        } elseif(is_object($where)) {
            $where  =   get_object_vars($where);
        }

        if(is_string($where) && '' != $where) {
            $map    =   array();
            $map['_string']   =   $where;
            $where  =   $map;
        }        
        if(isset($this->options['where'])) {
            $this->options['where'] = array_merge($this->options['where'],$where);
        } else {
            $this->options['where'] = $where;
        }
        return $this;
    }

    // 指定查询数量
    public function limit($offset, $length=null)
	{
        if(is_null($length) && strpos($offset,',')) 
        {
            list($offset,$length) = explode(',', $offset);
        }
        $this->options['limit']   = intval($offset).($length ? ',' . intval($length) : '');
        return $this;
    }

    // 指定分页
    public function page($page, $listRows=null)
	{
        if(is_null($listRows) && strpos($page, ','))
        {
            list($page,$listRows) = explode(',',$page);
        }
        $this->options['page']    = array(intval($page), intval($listRows));
        return $this;
    }

    // 查询注释
    public function comment($comment)
	{
        $this->options['comment'] = $comment;
        return $this;
    }

    // 获取执行的SQL语句
    public function fetchSql($fetch=true)
	{
        $this->options['fetch_sql'] =   $fetch;
        return $this;
    }

    // 参数绑定
    public function bind($key, $value=false) 
	{
        if(is_array($key)) {
            $this->options['bind'] = $key;
        } else {
            $num =  func_num_args();
            if($num>2) {
                $params = func_get_args();
                array_shift($params);
                $this->options['bind'][$key] = $params;
            } else {
                $this->options['bind'][$key] = $value;
            }        
        }
        return $this;
    }

    // 设置模型的属性值
    public function setProperty($name, $value) 
	{
        if(property_exists($this, $name))
            $this->$name = $value;
        return $this;
    }

}
