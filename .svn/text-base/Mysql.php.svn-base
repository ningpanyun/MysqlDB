<?php
namespace MysqlDB;

use MysqlDB\Conf\Config;
use MysqlDB\Conf\Common;
use Exception;
use PDO;

class Mysql
{
    protected $PDOStatement = null;		// PDO操作实例
    protected $model      = '';			// 当前操作所属的模型名
    protected $queryStr   = '';			// 当前SQL指令
    protected $modelSql   = array();
    protected $lastInsID  = null;		// 最后插入ID
    protected $numRows    = 0;			// 返回或者影响记录数
    protected $transTimes = 0;			// 事务指令数
    protected $error      = '';			// 错误信息
    protected $linkID     = null;		// 当前连接ID
    protected $config     = array();	// 数据库连接参数配置
    // 数据库表达式
    protected $exp = array('eq'=>'=','neq'=>'<>','gt'=>'>','egt'=>'>=','lt'=>'<','elt'=>'<=','notlike'=>'NOT LIKE','like'=>'LIKE','in'=>'IN','notin'=>'NOT IN','not in'=>'NOT IN','between'=>'BETWEEN','not between'=>'NOT BETWEEN','notbetween'=>'NOT BETWEEN');
    // 查询表达式
    protected $selectSql  = 'SELECT%DISTINCT% %FIELD% FROM %TABLE%%FORCE%%JOIN%%WHERE%%GROUP%%HAVING%%ORDER%%LIMIT% %UNION%%LOCK%%COMMENT%';
    protected $queryTimes   =   0;		// 查询次数
    protected $executeTimes =   0;		// 执行次数
    // PDO连接参数
    protected $options = array(
        PDO::ATTR_CASE              =>  PDO::CASE_LOWER,
        PDO::ATTR_ERRMODE           =>  PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS      =>  PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES =>  false,
    );
    protected $bind         =   array(); // 参数绑定

    // 架构函数 读取数据库配置信息
    public function __construct($config='')
	{
        $this->config = $config;
		if(isset($this->config['params']) && is_array($this->config['params']))
		{
			$this->options = $this->config['params'] + $this->options;
		}
    }

    // 连接数据库方法
    public function connect($config='', $linkNum=0, $autoConnection=false) 
	{
        if(! $this->linkID) 
		{
            if(empty($config))  $config = $this->config;
            try {
                if(empty($config['dsn'])) 
				{
                    $config['dsn']  =   $this->parseDsn($config);
                }
                $this->linkID = new PDO($config['dsn'], $config['username'], $config['password'], $this->options);
            } catch (\PDOException $e) {
				return new \PDOException($e->getMessage());
            }
        }
        return $this->linkID;
    }

    // 解析pdo连接的dsn信息
    protected function parseDsn($config)
	{
		$dsn = 'mysql:dbname=' . $config['database'] . ';host=' . $config['hostname'];
        if(!empty($config['hostport'])) {
            $dsn .= ';port=' . $config['hostport'];
        } elseif(!empty($config['socket'])) {
            $dsn .= ';unix_socket=' . $config['socket'];
        }

        if(! empty($config['charset']))
		{
            $this->options[PDO::MYSQL_ATTR_INIT_COMMAND] = 'SET NAMES ' . $config['charset'];
            $dsn .= ';charset=' . $config['charset'];
        }
        return $dsn;
	}

    // 释放查询结果
    public function free() 
	{
        $this->PDOStatement = null;
    }
	
	// 取得数据表的字段信息
    public function getFields($tableName) 
	{
        $this->initConnect();
        list($tableName) = explode(' ', $tableName);
        if(strpos($tableName,'.')) {
        	list($dbName,$tableName) = explode('.', $tableName);
			$sql   = 'SHOW COLUMNS FROM `' . $dbName . '`.`' . $tableName . '`';
        } else {
        	$sql   = 'SHOW COLUMNS FROM `' . $tableName . '`';
        }
        
        $result = $this->query($sql);
        $info   =   array();
        if($result) {
            foreach ($result as $key => $val) 
			{
				if(PDO::CASE_LOWER != $this->linkID->getAttribute(PDO::ATTR_CASE))
				{
					$val = array_change_key_case ($val, CASE_LOWER);
				}
                $info[$val['field']] = array(
                    'name'    => $val['field'],
                    'type'    => $val['type'],
                    'notnull' => (bool) ($val['null'] === ''),
                    'default' => $val['default'],
                    'primary' => (strtolower($val['key']) == 'pri'),
                    'autoinc' => (strtolower($val['extra']) == 'auto_increment'),
                );
            }
        }
        return $info;
    }
	
    // 执行查询 返回数据集
    public function query($str, $fetchSql=false) 
	{
        $this->initConnect();
        if (! $this->linkID) return false;
        $this->queryStr = $str;
        if(! empty($this->bind))
		{
            $that = $this;
            $this->queryStr =   strtr($this->queryStr, array_map(function($val) use($that) { return '\''.$that->escapeString($val).'\''; }, $this->bind));
        }
		
        if($fetchSql) return $this->queryStr;
		
        //释放前次的查询结果
        if (! empty($this->PDOStatement)) $this->free();
        $this->queryTimes ++;
		
        $this->PDOStatement = $this->linkID->prepare($str);
        if(false === $this->PDOStatement)
		{
            $this->error();
            return false;
        }
		
        foreach ($this->bind as $key => $val)
		{
            if(is_array($val)) {
                $this->PDOStatement->bindValue($key, $val[0], $val[1]);
            } else {
                $this->PDOStatement->bindValue($key, $val);
            }
        }
		
        $this->bind =   array();
        try {
            $result =   $this->PDOStatement->execute();
            $this->modelSql[$this->model]   =  $this->queryStr;

            if (false === $result) {
                $this->error();
                return false;
            } else {
                return $this->getResult();
            }
        } catch (\PDOException $e) {
            $this->error();
            return false;
        }
    }

    // 执行语句
    public function execute($str, $fetchSql=false) 
	{
        $this->initConnect();
        if (! $this->linkID) return false;
        $this->queryStr = $str;
        if(! empty($this->bind))
		{
            $that = $this;
            $this->queryStr =   strtr($this->queryStr,array_map(function($val) use($that){ return '\''.$that->escapeString($val).'\''; },$this->bind));
        }
        if($fetchSql) return $this->queryStr;
		
        //释放前次的查询结果
        if (! empty($this->PDOStatement)) $this->free();
        $this->executeTimes++;

        $this->PDOStatement =   $this->linkID->prepare($str);
        if(false === $this->PDOStatement) 
		{
            $this->error();
            return false;
        }	
			
        foreach ($this->bind as $key => $val) 
		{
            if(is_array($val)) {
                $this->PDOStatement->bindValue($key, $val[0], $val[1]);
            } else {
                $this->PDOStatement->bindValue($key, $val);
            }
        }
		
        $this->bind =   array();
        try {
            $result =   $this->PDOStatement->execute();
            $this->modelSql[$this->model]   =  $this->queryStr;
            if (false === $result) {
                $this->error();
                return false;
            } else {
                $this->numRows = $this->PDOStatement->rowCount();
                if(preg_match("/^\s*(INSERT\s+INTO|REPLACE\s+INTO)\s+/i", $str)) 
				{
                    $this->lastInsID = $this->linkID->lastInsertId();
                }
                return $this->numRows;
            }
        } catch (\PDOException $e) {
            $this->error();
            return false;
        }
    }

    // 启动事务
    public function startTrans() 
	{
        $this->initConnect();
        if (! $this->linkID) return false;
        
		if ($this->transTimes == 0)
		{
            $this->linkID->beginTransaction();
        }
        $this->transTimes ++;
        return ;
    }

    // 用于非自动提交状态下面的查询提交
    public function commit() 
	{
        if ($this->transTimes > 0) 
		{
            $this->transTimes = 0;
            if(! $this->linkID->commit())
			{
                $this->error();
                return false;
            }
        }
        return true;
    }

    // 事务回滚
    public function rollback() 
	{
        if ($this->transTimes > 0) 
		{
            $this->transTimes = 0;
            if(! $this->linkID->rollback())
			{
                $this->error();
                return false;
            }
        }
        return true;
    }

    // 获得所有的查询数据
    private function getResult()
	{
        $result = $this->PDOStatement->fetchAll(PDO::FETCH_ASSOC);
        $this->numRows = count($result);
        return $result;
    }

    // 获得查询次数
    public function getQueryTimes($execute=false)
	{
        return $execute ? $this->queryTimes+$this->executeTimes : $this->queryTimes;
    }

    // 获得执行次数
    public function getExecuteTimes()
	{
        return $this->executeTimes;
    }

    // 关闭数据库
    public function close()
	{
        $this->linkID = null;
    }

    // 数据库错误信息
    public function error() 
	{
        if($this->PDOStatement) {
            $error = $this->PDOStatement->errorInfo();
            $this->error = $error[1] . ':' . $error[2];
        } else {
            $this->error = '';
        }
        if('' != $this->queryStr)
		{
            $this->error .= "\n [ SQL语句 ] : " . $this->queryStr;
        }
        return $this->error;
    }

    // 设置锁机制
    protected function parseLock($lock = false)
	{
        return $lock ? ' FOR UPDATE ' : '';
    }

    // set分析
    protected function parseSet($data) 
	{
        foreach ($data as $key=>$val)
		{
            if(is_array($val) && 'exp' == $val[0]) {
                $set[]  =   $this->parseKey($key) . '=' . $val[1];
            } elseif(is_null($val)) {
                $set[]  =   $this->parseKey($key) . '=NULL';
            } elseif(is_scalar($val)) {// 过滤非标量数据
                if(0===strpos($val,':') && in_array($val,array_keys($this->bind)) ){
                    $set[]  =   $this->parseKey($key).'='.$this->escapeString($val);
                }else{
                    $name   =   count($this->bind);
                    $set[]  =   $this->parseKey($key).'=:'.$name;
                    $this->bindParam($name,$val);
                }
            }
        }
        return ' SET '.implode(',',$set);
    }

    // 参数绑定
    protected function bindParam($name, $value)
	{
        $this->bind[':'.$name]  =   $value;
    }

    // 字段名分析
    protected function parseKey(&$key) 
	{
        $key   =  trim($key);
        if(!is_numeric($key) && !preg_match('/[,\'\"\*\(\)`.\s]/',$key)) {
           $key = '`'.$key.'`';
        }
        return $key;
    }
    
    // value分析
    protected function parseValue($value) 
	{
        if(is_string($value)) {
            $value =  strpos($value,':') === 0 && in_array($value,array_keys($this->bind))? $this->escapeString($value) : '\''.$this->escapeString($value).'\'';
        } elseif(isset($value[0]) && is_string($value[0]) && strtolower($value[0]) == 'exp') {
            $value =  $this->escapeString($value[1]);
        } elseif(is_array($value)) {
            $value =  array_map(array($this, 'parseValue'),$value);
        } elseif(is_bool($value)) {
            $value =  $value ? '1' : '0';
        } elseif(is_null($value)) {
            $value =  'null';
        }
        return $value;
    }

    // field分析
    protected function parseField($fields) 
	{
        if(is_string($fields) && '' !== $fields) 
		{
            $fields    = explode(',',$fields);
        }
		
        if(is_array($fields)) 
		{
            $array   =  array();
            foreach ($fields as $key=>$field)
			{
                if(!is_numeric($key))
                    $array[] =  $this->parseKey($key).' AS '.$this->parseKey($field);
                else
                    $array[] =  $this->parseKey($field);
            }
            $fieldsStr = implode(',', $array);
        } else {
            $fieldsStr = '*';
        }
        return $fieldsStr;
    }

    // table分析
    protected function parseTable($tables) 
	{
        if(is_array($tables)) 
		{
            $array   =  array();
            foreach ($tables as $table=>$alias) {
                if(!is_numeric($table))
                    $array[] =  $this->parseKey($table).' '.$this->parseKey($alias);
                else
                    $array[] =  $this->parseKey($alias);
            }
            $tables  =  $array;
        } elseif(is_string($tables)) {
            $tables  =  explode(',',$tables);
            array_walk($tables, array(&$this, 'parseKey'));
        }
        return implode(',',$tables);
    }

    // where分析
    protected function parseWhere($where)
	{
        $whereStr = '';
        if(is_string($where)) {
            $whereStr = $where;
        } else {
            $operate  = isset($where['_logic'])?strtoupper($where['_logic']):'';
            if(in_array($operate,array('AND','OR','XOR'))){
                $operate    =   ' '.$operate.' ';
                unset($where['_logic']);
            } else {
                $operate    =   ' AND ';
            }
            foreach ($where as $key=>$val)
			{
                if(is_numeric($key)){
                    $key  = '_complex';
                }
                if(0===strpos($key,'_')) {
                    // 解析特殊条件表达式
                    $whereStr   .= $this->parseThinkWhere($key,$val);
                }else{
                    // 多条件支持
                    $multi  = is_array($val) &&  isset($val['_multi']);
                    $key    = trim($key);
                    if(strpos($key,'|')) { // 支持 name|title|nickname 方式定义查询字段
                        $array =  explode('|',$key);
                        $str   =  array();
                        foreach ($array as $m=>$k){
                            $v =  $multi?$val[$m]:$val;
                            $str[]   = $this->parseWhereItem($this->parseKey($k),$v);
                        }
                        $whereStr .= '( '.implode(' OR ',$str).' )';
                    }elseif(strpos($key,'&')){
                        $array =  explode('&',$key);
                        $str   =  array();
                        foreach ($array as $m=>$k){
                            $v =  $multi?$val[$m]:$val;
                            $str[]   = '('.$this->parseWhereItem($this->parseKey($k),$v).')';
                        }
                        $whereStr .= '( '.implode(' AND ',$str).' )';
                    }else{
                        $whereStr .= $this->parseWhereItem($this->parseKey($key),$val);
                    }
                }
                $whereStr .= $operate;
            }
            $whereStr = substr($whereStr,0,-strlen($operate));
        }
        return empty($whereStr)?'':' WHERE '.$whereStr;
    }

    // where子单元分析
    protected function parseWhereItem($key, $val)
	{
        $whereStr = '';
        if(is_array($val)) {
            if(is_string($val[0])) {
				$exp	=	strtolower($val[0]);
                if(preg_match('/^(eq|neq|gt|egt|lt|elt)$/',$exp)) { // 比较运算
                    $whereStr .= $key.' '.$this->exp[$exp].' '.$this->parseValue($val[1]);
                }elseif(preg_match('/^(notlike|like)$/',$exp)){// 模糊查找
                    if(is_array($val[1])) {
                        $likeLogic  =   isset($val[2])?strtoupper($val[2]):'OR';
                        if(in_array($likeLogic,array('AND','OR','XOR'))){
                            $like       =   array();
                            foreach ($val[1] as $item){
                                $like[] = $key.' '.$this->exp[$exp].' '.$this->parseValue($item);
                            }
                            $whereStr .= '('.implode(' '.$likeLogic.' ',$like).')';                          
                        }
                    }else{
                        $whereStr .= $key.' '.$this->exp[$exp].' '.$this->parseValue($val[1]);
                    }
                }elseif('bind' == $exp ){ // 使用表达式
                    $whereStr .= $key.' = :'.$val[1];
                }elseif('exp' == $exp ){ // 使用表达式
                    $whereStr .= $key.' '.$val[1];
                }elseif(preg_match('/^(notin|not in|in)$/',$exp)){ // IN 运算
                    if(isset($val[2]) && 'exp'==$val[2]) {
                        $whereStr .= $key.' '.$this->exp[$exp].' '.$val[1];
                    }else{
                        if(is_string($val[1])) {
                             $val[1] =  explode(',',$val[1]);
                        }
                        $zone      =   implode(',',$this->parseValue($val[1]));
                        $whereStr .= $key.' '.$this->exp[$exp].' ('.$zone.')';
                    }
                }elseif(preg_match('/^(notbetween|not between|between)$/',$exp)){ // BETWEEN运算
                    $data = is_string($val[1])? explode(',',$val[1]):$val[1];
                    $whereStr .=  $key.' '.$this->exp[$exp].' '.$this->parseValue($data[0]).' AND '.$this->parseValue($data[1]);
                }else{
					return new Exception(':' . $val[0]);
                }
            }else {
                $count = count($val);
                $rule  = isset($val[$count-1]) ? (is_array($val[$count-1]) ? strtoupper($val[$count-1][0]) : strtoupper($val[$count-1]) ) : '' ; 
                if(in_array($rule,array('AND','OR','XOR'))) {
                    $count  = $count -1;
                }else{
                    $rule   = 'AND';
                }
                for($i=0;$i<$count;$i++) {
                    $data = is_array($val[$i])?$val[$i][1]:$val[$i];
                    if('exp'==strtolower($val[$i][0])) {
                        $whereStr .= $key.' '.$data.' '.$rule.' ';
                    }else{
                        $whereStr .= $this->parseWhereItem($key,$val[$i]).' '.$rule.' ';
                    }
                }
                $whereStr = '( '.substr($whereStr,0,-4).' )';
            }
        } else {
            $whereStr .= $key . ' = ' . $this->parseValue($val);
        }
        return $whereStr;
    }

    // 特殊条件分析
    protected function parseThinkWhere($key,$val) 
	{
        $whereStr   = '';
        switch($key) {
            case '_string':
                // 字符串模式查询条件
                $whereStr = $val;
                break;
            case '_complex':
                // 复合查询条件
                $whereStr = substr($this->parseWhere($val),6);
                break;
            case '_query':
                // 字符串模式查询条件
                parse_str($val,$where);
                if(isset($where['_logic'])) {
                    $op   =  ' '.strtoupper($where['_logic']).' ';
                    unset($where['_logic']);
                }else{
                    $op   =  ' AND ';
                }
                $array   =  array();
                foreach ($where as $field=>$data)
                    $array[] = $this->parseKey($field).' = '.$this->parseValue($data);
                $whereStr   = implode($op,$array);
                break;
        }
        return '( '.$whereStr.' )';
    }

    // limit分析
    protected function parseLimit($limit)
	{
        return !empty($limit) ? ' LIMIT '.$limit.' ':'';
    }

    // join分析
    protected function parseJoin($join) 
	{
        $joinStr = '';
        if(!empty($join)) {
            $joinStr    =   ' '.implode(' ',$join).' ';
        }
        return $joinStr;
    }

    // order分析
    protected function parseOrder($order) 
	{
        if(is_array($order)) {
            $array   =  array();
            foreach ($order as $key=>$val){
                if(is_numeric($key)) {
                    $array[] =  $this->parseKey($val);
                }else{
                    $array[] =  $this->parseKey($key).' '.$val;
                }
            }
            $order   =  implode(',',$array);
        }
        return !empty($order)?  ' ORDER BY '.$order:'';
    }

    // group分析
    protected function parseGroup($group) 
	{
        return !empty($group)? ' GROUP BY '.$group:'';
    }

    // having分析
    protected function parseHaving($having) 
	{
        return  !empty($having) ? ' HAVING '.$having:'';
    }

    // comment分析
    protected function parseComment($comment)
	{
        return  !empty($comment)?   ' /* '.$comment.' */':'';
    }

    // distinct分析
    protected function parseDistinct($distinct) 
	{
        return !empty($distinct)?   ' DISTINCT ' :'';
    }

    // union分析
    protected function parseUnion($union) 
	{
        if(empty($union)) return '';
        if(isset($union['_all'])) {
            $str  =   'UNION ALL ';
            unset($union['_all']);
        }else{
            $str  =   'UNION ';
        }
        foreach ($union as $u){
            $sql[] = $str.(is_array($u)?$this->buildSelectSql($u):$u);
        }
        return implode(' ',$sql);
    }

    // 参数绑定分析
    protected function parseBind($bind){
        $this->bind   =   array_merge($this->bind,$bind);
    }

    // index分析，可在操作链中指定需要强制使用的索引
    protected function parseForce($index) 
	{
        if(empty($index)) return '';
        if(is_array($index)) $index = join(",", $index);
        return sprintf(" FORCE INDEX ( %s ) ", $index);
    }
	
	// 取得数据库的表信息
    public function getTables($dbName='') 
	{
        $sql    = !empty($dbName)?'SHOW TABLES FROM '.$dbName:'SHOW TABLES ';
        $result = $this->query($sql);
        $info   =   array();
        foreach ($result as $key => $val) {
            $info[$key] = current($val);
        }
        return $info;
    }

    // ON DUPLICATE KEY UPDATE 分析
    protected function parseDuplicate($duplicate)
	{
        // 布尔值或空则返回空字符串
        if(is_bool($duplicate) || empty($duplicate)) return '';
        
        if(is_string($duplicate)){
        	// field1,field2 转数组
        	$duplicate = explode(',', $duplicate);
        }elseif(is_object($duplicate)){
        	// 对象转数组
        	$duplicate = get_class_vars($duplicate);
        }
        $updates                    = array();
        foreach((array) $duplicate as $key=>$val){
            if(is_numeric($key)){ // array('field1', 'field2', 'field3') 解析为 ON DUPLICATE KEY UPDATE field1=VALUES(field1), field2=VALUES(field2), field3=VALUES(field3)
                $updates[]          = $this->parseKey($val)."=VALUES(".$this->parseKey($val).")";
            }else{
                if(is_scalar($val)) // 兼容标量传值方式
                    $val            = array('value', $val);
                if(!isset($val[1])) continue;
                switch($val[0]){
                    case 'exp': // 表达式
                        $updates[]  = $this->parseKey($key)."=($val[1])";
                        break;
                    case 'value': // 值
                    default:
                        $name       = count($this->bind);
                        $updates[]  = $this->parseKey($key)."=:".$name;
                        $this->bindParam($name, $val[1]);
                        break;
                }
            }
        }
        if(empty($updates)) return '';
        return " ON DUPLICATE KEY UPDATE ".join(', ', $updates);
    }
	
	// 执行存储过程查询 返回多个数据集
    public function procedure($str,$fetchSql=false) 
	{
        $this->initConnect(false);
        $this->linkID->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
        if ( !$this->linkID ) return false;
        $this->queryStr     =   $str;
        if($fetchSql){
            return $this->queryStr;
        }
        //释放前次的查询结果
        if ( !empty($this->PDOStatement) ) $this->free();
        $this->queryTimes++;
        $this->PDOStatement = $this->linkID->prepare($str);
        if(false === $this->PDOStatement){
            $this->error();
            return false;
        }
        try{
            $result = $this->PDOStatement->execute();
            $this->modelSql[$this->model]   =  $this->queryStr;
            do {
                $result = $this->PDOStatement->fetchAll(PDO::FETCH_ASSOC);
                if ($result)
                {
                    $resultArr[] = $result;
                }
            }
            while ($this->PDOStatement->nextRowset());
            $this->linkID->setAttribute(PDO::ATTR_ERRMODE, $this->options[PDO::ATTR_ERRMODE]);
            return $resultArr;
        }catch (\PDOException $e) {
            $this->error();
            $this->linkID->setAttribute(PDO::ATTR_ERRMODE, $this->options[PDO::ATTR_ERRMODE]);
            return false;
        }
    }

    // 插入记录
    public function insert($data,$options=array(),$replace=false) 
	{
        $values  =  $fields    = array();
        $this->model  =   $options['model'];
        $this->parseBind(!empty($options['bind'])?$options['bind']:array());
        foreach ($data as $key=>$val){
            if(is_array($val) && 'exp' == $val[0]){
                $fields[]   =  $this->parseKey($key);
                $values[]   =  $val[1];
            }elseif(is_null($val)){
                $fields[]   =   $this->parseKey($key);
                $values[]   =   'NULL';
            }elseif(is_scalar($val)) { // 过滤非标量数据
                $fields[]   =   $this->parseKey($key);
                if(0===strpos($val,':') && in_array($val,array_keys($this->bind))){
                    $values[]   =   $this->parseValue($val);
                }else{
                    $name       =   count($this->bind);
                    $values[]   =   ':'.$name;
                    $this->bindParam($name,$val);
                }
            }
        }
        // 兼容数字传入方式
        $replace= (is_numeric($replace) && $replace>0)?true:$replace;
        $sql    = (true===$replace?'REPLACE':'INSERT').' INTO '.$this->parseTable($options['table']).' ('.implode(',', $fields).') VALUES ('.implode(',', $values).')'.$this->parseDuplicate($replace);
        $sql    .= $this->parseComment(!empty($options['comment'])?$options['comment']:'');
        return $this->execute($sql,!empty($options['fetch_sql']) ? true : false);
    }

    // 批量插入记录
    public function insertAll($dataSet,$options=array(),$replace=false)
	{
        $values  =  array();
        $this->model  =   $options['model'];
        if(!is_array($dataSet[0])) return false;
        $this->parseBind(!empty($options['bind'])?$options['bind']:array());
        $fields =   array_map(array($this,'parseKey'),array_keys($dataSet[0]));
        foreach ($dataSet as $data){
            $value   =  array();
            foreach ($data as $key=>$val){
                if(is_array($val) && 'exp' == $val[0]){
                    $value[]   =  $val[1];
                }elseif(is_null($val)){
                    $value[]   =   'NULL';
                }elseif(is_scalar($val)){
                    if(0===strpos($val,':') && in_array($val,array_keys($this->bind))){
                        $value[]   =   $this->parseValue($val);
                    }else{
                        $name       =   count($this->bind);
                        $value[]   =   ':'.$name;
                        $this->bindParam($name,$val);
                    }
                }
            }
            $values[]    = '('.implode(',', $value).')';
        }
        // 兼容数字传入方式
        $replace= (is_numeric($replace) && $replace>0)?true:$replace;
        $sql    =  (true===$replace?'REPLACE':'INSERT').' INTO '.$this->parseTable($options['table']).' ('.implode(',', $fields).') VALUES '.implode(',',$values).$this->parseDuplicate($replace);
        $sql    .= $this->parseComment(!empty($options['comment'])?$options['comment']:'');
        return $this->execute($sql,!empty($options['fetch_sql']) ? true : false);
    }

    // 更新记录
    public function update($data, $options)
	{
        $this->model  =   $options['model'];
        $this->parseBind(!empty($options['bind'])?$options['bind']:array());
        $table  =   $this->parseTable($options['table']);
        $sql   = 'UPDATE ' . $table . $this->parseSet($data);
        if(strpos($table,',')){// 多表更新支持JOIN操作
            $sql .= $this->parseJoin(!empty($options['join'])?$options['join']:'');
        }
        $sql .= $this->parseWhere(!empty($options['where'])?$options['where']:'');
        if(!strpos($table,',')){
            //  单表更新支持order和lmit
            $sql   .=  $this->parseOrder(!empty($options['order'])?$options['order']:'')
                .$this->parseLimit(!empty($options['limit'])?$options['limit']:'');
        }
        $sql .=   $this->parseComment(!empty($options['comment'])?$options['comment']:'');
        return $this->execute($sql,!empty($options['fetch_sql']) ? true : false);
    }

    // 删除记录
    public function delete($options=array()) 
	{
        $this->model  =   $options['model'];
        $this->parseBind(!empty($options['bind'])?$options['bind']:array());
        $table  =   $this->parseTable($options['table']);
        $sql    =   'DELETE FROM '.$table;
        if(strpos($table,',')){// 多表删除支持USING和JOIN操作
            if(!empty($options['using'])){
                $sql .= ' USING '.$this->parseTable($options['using']).' ';
            }
            $sql .= $this->parseJoin(!empty($options['join'])?$options['join']:'');
        }
        $sql .= $this->parseWhere(!empty($options['where'])?$options['where']:'');
        if(!strpos($table,',')){
            // 单表删除支持order和limit
            $sql .= $this->parseOrder(!empty($options['order'])?$options['order']:'')
            .$this->parseLimit(!empty($options['limit'])?$options['limit']:'');
        }
        $sql .=   $this->parseComment(!empty($options['comment'])?$options['comment']:'');
        return $this->execute($sql,!empty($options['fetch_sql']) ? true : false);
    }

    // 查找记录
    public function select($options=array())
	{
        $this->model  =   $options['model'];
        $this->parseBind(!empty($options['bind'])?$options['bind']:array());
        $sql		= $this->buildSelectSql($options);
        $result  	= $this->query($sql,!empty($options['fetch_sql']) ? true : false);
        return $result;
    }

    // 生成查询SQL
    public function buildSelectSql($options=array())
	{
        if(isset($options['page'])) {
            // 根据页数计算limit
            list($page,$listRows)   =   $options['page'];
            $page    =  $page>0 ? $page : 1;
            $listRows=  $listRows>0 ? $listRows : (is_numeric($options['limit'])?$options['limit']:20);
            $offset  =  $listRows*($page-1);
            $options['limit'] =  $offset.','.$listRows;
        }
        $sql  =   $this->parseSql($this->selectSql,$options);
        return $sql;
    }

    // 替换SQL语句中表达式
    public function parseSql($sql,$options=array())
	{
        $sql   = str_replace(
            array('%TABLE%','%DISTINCT%','%FIELD%','%JOIN%','%WHERE%','%GROUP%','%HAVING%','%ORDER%','%LIMIT%','%UNION%','%LOCK%','%COMMENT%','%FORCE%'),
            array(
                $this->parseTable($options['table']),
                $this->parseDistinct(isset($options['distinct'])?$options['distinct']:false),
                $this->parseField(!empty($options['field'])?$options['field']:'*'),
                $this->parseJoin(!empty($options['join'])?$options['join']:''),
                $this->parseWhere(!empty($options['where'])?$options['where']:''),
                $this->parseGroup(!empty($options['group'])?$options['group']:''),
                $this->parseHaving(!empty($options['having'])?$options['having']:''),
                $this->parseOrder(!empty($options['order'])?$options['order']:''),
                $this->parseLimit(!empty($options['limit'])?$options['limit']:''),
                $this->parseUnion(!empty($options['union'])?$options['union']:''),
                $this->parseLock(isset($options['lock'])?$options['lock']:false),
                $this->parseComment(!empty($options['comment'])?$options['comment']:''),
                $this->parseForce(!empty($options['force'])?$options['force']:'')
            ),$sql);
        return $sql;
    }

    // 获取最近一次查询的sql语句 
    public function getLastSql($model='') 
	{
        return $model && isset($this->modelSql[$model]) ? $this->modelSql[$model] : $this->queryStr;
    }

    // 获取最近插入的ID
    public function getLastInsID()
	{
        return $this->lastInsID;
    }

    // 获取最近的错误信息
    public function getError()
	{
        return $this->error;
    }

    // SQL指令安全过滤
    public function escapeString($str)
	{
        return addslashes($str);
    }

    // 设置当前操作模型
    public function setModel($model)
	{
        $this->model =  $model;
    }

    // 初始化数据库连接
    protected function initConnect() 
	{
		if(! $this->linkID ) $this->linkID = $this->connect();
    }
 
   	// 析构方法
    public function __destruct() 
	{
        if ($this->PDOStatement) {
            $this->free();
        }
        $this->close();
    }
}
