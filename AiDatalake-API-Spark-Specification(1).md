# Spark API 文档

文档版本: 01  
发布日期: 2026-05-20

---

## 目录

### 1. Spark 作业相关 API

- [1.1 查询 Spark 作业状态](#11-查询-spark-作业状态)
- [1.2 启动 Spark 作业](#12-启动-spark-作业)
- [1.3 查询 Spark 作业列表](#13-查询-spark-作业列表)
- [1.4 查看 Spark 作业详情](#14-查看-spark-作业详情)
- [1.5 取消 Spark 作业](#15-取消-spark-作业)

### 2. SparkSql 作业相关 API

- [2.1 查询 SparkSql 作业状态](#21-查询-sparksql-作业状态)
- [2.2 查询 SparkSql 作业列表](#22-查询-sparksql-作业列表)
- [2.3 查看 SparkSql 作业详情](#23-查看-sparksql-作业详情)
- [2.4 取消 SparkSql 作业](#24-取消-sparksql-作业)
- [2.5 检查 SparkSql 语法](#25-检查-sparksql-语法)
- [2.6 执行 SparkSql 作业](#26-执行-sparksql-作业)
- [2.7 预览 SparkSql 作业查询结果](#27-预览-sparksql-作业查询结果)
- [2.8 重启 SparkSql 集群](#28-重启-sparksql-集群)
- [2.9 查询重启 SparkSql 集群状态](#29-查询重启-sparksql-集群状态)

---

## 1. Spark 作业相关 API

### 1.1 查询 Spark 作业状态

#### 功能介绍

该API用于查询Spark作业的状态。

#### 调用方法

请参见如何调用API。

#### 授权信息

账号具备所有API的调用权限,如果使用账号下的IAM用户调用当前API,该IAM用户需具备调用API所需的权限。

- 如果使用角色与策略授权,具体权限要求请参见权限和授权项。
- 如果使用身份策略授权,需具备如下身份策略权限。

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkJob:show | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkJob:show | Read | sparkJob * | - | - | - |

#### URI

```
GET https://localhost.com/v2/workspaces/{workspace_id}/spark-jobs/{job_id}/state
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| job_id | 是 | String | Spark作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| job_id | String | Spark作业的ID,采用UUID(通用唯一识别码)格式。取值范围:UUID格式,长度为36个字符。 |
| state | String | Spark作业的状态。取值范围:"QUEUED"(排队中)、"PENDING"(等待中)、"RUNNING"(运行中)、"CANCELING"(取消中)、"CANCELED"(已取消)、"FAILED"(已失败)、"QUEUED_TIMEOUT"(排队超时)、"RUNNING_TIMEOUT"(运行超时)、"SUCCEED"(已成功)。 |

**状态码: 400/401/403**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 1.2 启动 Spark 作业

#### 功能介绍

该API用于启动Spark作业,包含spark jar作业和Python作业和SQL Script作业。

#### 调用方法

请参见如何调用API。

#### 授权信息

账号具备所有API的调用权限,如果使用账号下的IAM用户调用当前API,该IAM用户需具备调用API所需的权限。

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkJob:run | Write | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkJob:run | Write | sparkJob * | - | - | - |
| DataArtsFabric:sparkJob:run | Write | endpoint * | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/spark-jobs
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |

**Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| X-Client-Token | 否 | String | 服务事务ID,用于链路追踪。 |

**Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| name | 是 | String | 用户指定的Spark作业名称。取值范围:长度为1-128个字符。 |
| endpoint_name | 是 | String | 端点名称。取值范围:长度为1-63个字符,只能由小写字母、数字及中划线组成,且必须以字母开头。 |
| job_config | 是 | SparkJobArtifact object | Spark作业配置,用于存储作业必要的相关信息。不同类型的作业需要不同的作业参数。 |
| spark_version | 是 | String | Serverless Spark版本。取值范围:长度为1-64个字符。 |
| job_agency | 否 | String | 自定义委托的委托名。用于作业操作OBS对象、转储日志、访问DLI元数据等。取值范围:长度为1-64个字符。 |
| resource_config | 否 | SparkResourceConfig object | Spark作业的资源配置。 |
| spark_config | 否 | Map<String,String> | 用户自定义Spark参数配置。取值范围:Map<string, string>,键值对个数不超过100个。 |
| image_config | 否 | SparkJobImageConfig object | Spark镜像相关配置。 |
| restore_strategy | 否 | SparkRestoreStrategy object | Spark作业的运行及恢复策略。 |
| description | 否 | String | 创建时用户指定的Spark作业描述信息。取值范围:长度为1-512个字符。 |
| labels | 否 | Array of SparkJobLabel objects | 作业标签。取值范围:最多16个标签。 |
| catalog_name | 否 | String | Catalog名称。用于指定作业使用的数据目录。取值范围:长度不超过128个字符。 |
| logging_config | 否 | SparkLoggingConfig object | Spark作业日志配置。 |

**SparkJobArtifact 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| job_type | 是 | String | 作业类型。取值范围:"spark_jar_job"(Spark jar作业)、"spark_python_job"(Python Spark作业)、"spark_sql_scripting_job"(SQL脚本作业,预留)。 |
| spark_jar_parameter | 否 | SparkJarParameter object | Spark Jar作业参数,用于配置Spark Jar类型作业的相关参数。 |
| spark_py_parameter | 否 | SparkPyParameter object | Spark Python作业参数,用于配置Spark Python类型作业的相关参数。 |
| spark_sql_scripting_parameter | 否 | SparkSQLScriptParameter object | Spark SQL脚本作业参数,用于配置Spark SQL Script类型作业的相关参数。 |

**SparkJarParameter 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| main_class | 否 | String | Spark Jar作业入口类。取值范围:长度为1-512个字符。 |
| main_args | 否 | Array of strings | Spark Jar作业入口类参数,多个参数之间空格分隔。取值范围:最多100个参数,每个参数长度为1-512个字符。 |
| main_jar | 否 | String | Spark Jar作业主类所在Jar包的OBS路径。当job_type是"spark_jar_job"时,必填。取值范围:长度为1-512个字符。 |
| dependency_jars | 否 | Array of strings | Spark作业依赖Jar包的OBS路径数组。示例:[obs://bucket_name/demo/test1.jar,obs://bucket_name/demo/test2.jar]。 |
| dependency_files | 否 | Array of strings | Spark作业依赖文件包的OBS路径数组。示例:[obs://bucket_name/demo/test11,obs://bucket_name/demo/test211.zip]。 |
| dependency_archives | 否 | Array of strings | Spark作业依赖archives包的OBS路径数组。示例:[obs://bucket_name/demo/testarchives11,obs://bucket_name/demo/test22]。 |
| dependency_py_files | 否 | Array of strings | Spark作业依赖python包OBS路径数组。示例:[obs://bucket_name/demo/testarchives11.zip,obs://bucket_name/demo/test3.py]。 |

**SparkPyParameter 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| main_python_file | 是 | String | Python Spark类型作业URL全路径。例如:obs://bucket/pyspark/pySpark_udf_python.py。 |
| main_args | 否 | Array of strings | Spark Python作业入口类参数,多个参数之间空格分隔。例如:--output obs://bucket/output/ --input obs://bucket/input/。 |
| dependency_jars | 否 | Array of strings | Spark作业依赖Jar包的OBS路径数组。 |
| dependency_files | 否 | Array of strings | Spark作业依赖文件包的OBS路径数组。 |
| dependency_archives | 否 | Array of strings | Spark作业依赖archives包的OBS路径数组。 |
| dependency_py_files | 否 | Array of strings | Spark作业依赖python包OBS路径数组。 |

**SparkSQLScriptParameter 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| sql_scripting_file | 是 | String | Spark Script SQL类型作业URL全路径。例如:obs://bucket/sparksqlsciprt/...。 |
| sql_scripting_parameters | 否 | Map<String,String> | 作业参数。属性中包含type和externalCatalog。 |
| dependency_jars | 否 | Array of strings | Spark作业依赖Jar包的OBS路径数组。 |
| sql_scripting_result_to_obs | 否 | Boolean | sql scripting结果是否写入OBS。取值范围:true(表示写入OBS)、false(表示不写入OBS)。默认取值:false。 |

**SparkResourceConfig 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| executor_number | 否 | Integer | Spark作业的最大executor个数。如果配置,则表示spark.dynamicAllocation.enabled=true,spark.dynamicAllocation.minExecutor=1,spark.dynamicAllocation.maxExecutors=executor_number,spark.dynamicAllocation.initialExecutors=1。取值范围:0-65535。 |
| driver_resource_spec | 否 | ResourceSpec object | 表示对作业使用CPU、内存、磁盘资源的包装,依据本数据结构可以生成对Driver、Executor等资源的粗粒度的描述。 |
| executor_resource_spec | 否 | ResourceSpec object | 表示对作业使用CPU、内存、磁盘资源的包装,依据本数据结构可以生成对Driver、Executor等资源的粗粒度的描述。 |

**ResourceSpec 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| cpu | 否 | Integer | CPU核数。取值范围:1-64。默认取值:1。 |
| memory | 否 | String | 内存大小。单位为MB或GB,默认为GB。取值范围:2-1024GB或等效MB值。默认取值:4GB。 |
| disk | 否 | Integer | Spark作业driver、executor的本地磁盘大小。取值范围:0-994。 |

**SparkJobImageConfig 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| image_feature | 否 | String | 用户作业使用的镜像类型。取值范围:"basic"(基础镜像)、"custom"(自定义镜像)。默认取值:basic。 |
| image_uri | 否 | String | 自定义镜像。当前只支持SWR,格式为:组织名/镜像名:镜像版本。当用户设置"image_feature"为"custom"时该参数生效。取值范围:长度为1-256个字符。 |

**SparkRestoreStrategy 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| max_retry | 否 | Integer | 作业失败后最大的重试次数。如果配置的值大于0,则自动开始失败重试;如果不配置或者配置为0,则不开启作业失败重试。取值范围:0-65535。 |
| retry_delay | 否 | Long | 作业失败重试机制中,每次重试的时间间隔。取值范围:默认30秒。 |
| queued_timeout | 否 | Long | 作业提交超时时间,如果超过此时间作业仍未运行,则作业失败。取值范围:默认3小时,最大1天,最小10分钟。 |
| running_timeout | 否 | Long | 作业运行超时时间,如果超过此时间作业还未运行结束,则作业会取消运行,并标记为运行超时。取值范围:默认不限制,最小10分钟,最大1年。 |

**SparkJobLabel 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| key | 是 | String | 标签的键。取值范围:长度为1-128个字符。 |
| value | 是 | String | 标签的值。取值范围:长度为1-128个字符。 |

**SparkLoggingConfig 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| driver_root_logger_level | 否 | String | Driver根目录日志级别配置。取值范围:DEBUG(debug)、TRACE(trace)、WARN(warning)、INFO(info)、ERROR(error)。 |
| driver_loggers_level_of_class | 否 | Array of SparkClassLoggerLevel objects | Driver输出日志类的名称和对应的日志级别配置。取值范围:最多20个。 |
| executor_root_logger_level | 否 | String | Executor根目录日志级别配置。取值范围:DEBUG(debug)、TRACE(trace)、WARN(warning)、INFO(info)、ERROR(error)。 |
| executor_loggers_level_of_class | 否 | Array of SparkClassLoggerLevel objects | Executor输出日志类的名称和对应的日志级别配置。取值范围:最多20个。 |

**SparkClassLoggerLevel 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| logger_name | 否 | String | 输出日志类的名称。取值范围:长度为1-256个字符。 |
| logger_level | 否 | String | 输出日志的级别。取值范围:DEBUG(debug)、TRACE(trace)、WARN(warning)、INFO(info)、ERROR(error)。 |

#### 响应参数

**状态码: 201**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| job_id | String | Spark作业ID,成功时返回。取值范围:长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 201 - 启动成功**

```json
{
  "job_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e"
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 201 | 启动成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 1.3 查询 Spark 作业列表

#### 功能介绍

该API用于查询Project下Spark作业列表。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkJob:list | List | sparkJob * | - | - | - |
| DataArtsFabric:sparkJob:list | List | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkJob:list | List | endpoint * | - | - | - |

#### URI

```
GET https://localhost.com/v2/workspaces/{workspace_id}/spark-jobs
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |

**Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| marker | 否 | String | 作业ID游标位置,用于分页查询。首次查询可不传或传空字符串,后续查询传入上次返回的next_marker值。取值范围:长度为36个字符的字母、数字、中划线及下划线的组合。 |
| limit | 否 | Integer | 查询记录数。取值范围:1-100。默认取值:10。 |
| create_time_after | 否 | Long | 用于查询创建时间在该时间点之后的作业。取值范围:1764061598000-253402271999000。 |
| create_time_before | 否 | Long | 用于查询创建时间在该时间点之前的作业。取值范围:1764061598000-253402271999000。 |
| endpoint_name | 否 | String | 端点名称。取值范围:长度为1-63个字符,只能由小写字母、数字及中划线组成,且必须以字母开头。 |
| job_id | 否 | String | Spark作业ID。取值范围:长度为1-64个字符的字母、数字、中划线及下划线的组合。 |
| name | 否 | String | 作业名称,支持模糊查询。取值范围:长度为0-128个字符。 |
| states | 否 | Array of strings | Spark作业状态。取值范围:"PENDING"(等待提交)、"QUEUED"(排队中)、"RUNNING"(运行中)、"CANCELING"(取消中)、"CANCELED"(已取消)、"FAILED"(已失败)、"SUCCEED"(已成功)、"QUEUED_TIMEOUT"(排队超时)、"RUNNING_TIMEOUT"(运行超时)。 |
| job_type | 否 | String | 作业类型。取值范围:"spark_jar_job"(Spark jar作业)、"spark_python_job"(Python Spark作业)、"spark_sql_scripting_job"(SQL脚本作业,预留)。 |
| create_user_id | 否 | String | Spark作业创建者。取值范围:长度为1-256个字符。 |
| labels | 否 | String | 指定作业标签作为过滤条件,支持多标签过滤。格式为"key=value",如:k1%3Dv1,"="需要转义为"%3D","k1"为标签键,"v1"为标签值。长度不超过12000个字符。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| page_info | SparkMarkerPageInfo object | 分页游标信息。 |
| jobs | Array of ListSparkJobResponseDto objects | 详细的Spark作业列表。 |

**SparkMarkerPageInfo 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| next_marker | String | 下一页游标,为空表示没有更多数据。取值范围:长度为36个字符的字母、数字、中划线及下划线的组合。 |
| current_count | Integer | 当前页数据条数。取值范围:大于等于0。 |

**ListSparkJobResponseDto 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| job_id | String | Spark作业的ID。取值范围:系统生成的唯一标识,长度为36个字符。 |
| client_token | String | Spark作业事务ID,防止重复提交。取值范围:UUID格式,长度为36个字符。 |
| name | String | Spark作业名称。取值范围:长度为0-128个字符。 |
| endpoint_name | String | 端点名称。取值范围:长度为0-63个字符。 |
| state | String | Spark作业的状态。取值范围:"PENDING"(等待提交)、"QUEUED"(排队中)、"RUNNING"(运行中)、"CANCELING"(取消中)、"CANCELED"(已取消)、"FAILED"(已失败)、"QUEUED_TIMEOUT"(提交超时)、"RUNNING_TIMEOUT"(运行超时)、"SUCCEED"(已成功)。 |
| job_type | String | 作业类型。取值范围:"spark_jar_job"(Spark jar作业)、"spark_python_job"(Python spark作业)、"spark_sql_scripting_job"(SQL脚本作业,预留)。 |
| retry_times | Long | 作业重试次数。取值范围:大于等于1。 |
| create_time | Long | 作业创建时间。取值范围:unix时间戳,单位毫秒。 |
| start_time | Long | 作业开始运行时间。取值范围:unix时间戳,单位毫秒。 |
| end_time | Long | 作业结束时间。取值范围:unix时间戳,单位毫秒。 |
| create_user_id | String | 启动作业的用户ID。取值范围:长度为0-256个字符。 |
| create_user_name | String | 启动作业的用户名称。取值范围:长度为0-256个字符。 |
| log_url | String | 日志归档路径OBS URL。取值范围:obs://开头的OBS路径。 |

**状态码: 400/401/403**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 200 - 查看Spark作业列表响应body体**

```json
{
  "page_info": {
    "next_marker": "marker_value",
    "current_count": 10
  },
  "jobs": [
    {
      "job_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e",
      "client_token": "token_value",
      "name": "test_job",
      "endpoint_name": "endpoint1",
      "state": "RUNNING",
      "job_type": "spark_jar_job",
      "retry_times": 0,
      "create_time": 1764061598000,
      "start_time": 1764061600000,
      "end_time": null,
      "create_user_id": "user123",
      "create_user_name": "test_user",
      "log_url": "obs://bucket/logs/"
    }
  ]
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 查看Spark作业列表响应body体 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 1.4 查看 Spark 作业详情

#### 功能介绍

查看Spark作业详情。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkJob:show | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkJob:show | Read | sparkJob * | - | - | - |

#### URI

```
GET https://localhost.com/v2/workspaces/{workspace_id}/spark-jobs/{job_id}
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| job_id | 是 | String | Spark作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| job_id | String | Spark作业的ID。取值范围:系统生成的唯一标识,长度为36个字符。 |
| client_token | String | Spark作业事务ID,防止重复提交。取值范围:UUID格式,长度为36个字符。 |
| name | String | Spark作业名称。取值范围:长度为0-128个字符。 |
| endpoint_name | String | 端点名称。取值范围:长度为0-63个字符。 |
| catalog_name | String | Catalog名称。作业使用的数据目录。取值范围:长度不超过128个字符。 |
| state | String | Spark作业的状态。取值范围:"PENDING"(已提交)、"QUEUED"(排队中)、"RUNNING"(运行中)、"CANCELING"(取消中)、"CANCELED"(已取消)、"FAILED"(已失败)、"QUEUED_TIMEOUT"(提交超时)、"RUNNING_TIMEOUT"(运行超时)、"SUCCEED"(已成功)。 |
| job_agency | String | 自定义委托的委托名。用于作业操作OBS对象、转储日志、访问DLI元数据等。取值范围:长度为0-64个字符。 |
| job_config | ShowSparkJobArtifactResponse object | Spark作业配置详情。 |
| resource_config | ShowSparkResourceConfigResponse object | driver_resource_spec:Driver资源规格。executor_resource_spec:Executor资源规格。 |
| image_config | ShowSparkJobImageConfigResponse object | Spark镜像相关配置。 |
| restore_strategy | ShowSparkRestoreStrategyResponse object | Spark作业的运行及恢复策略。 |
| retry_times | Long | 作业重试次数。取值范围:大于等于1。 |
| labels | Array of ShowSparkJobLabelResponse objects | 作业标签。取值范围:最多16个标签。 |
| logging_config | ShowSparkLoggingConfigResponse object | Spark作业日志配置。 |
| description | String | 创建时用户指定的Spark作业描述信息。取值范围:长度为0-512个字符。 |
| create_time | Long | 作业创建时间。取值范围:unix时间戳,单位毫秒。 |
| start_time | Long | 作业开始运行时间。取值范围:unix时间戳,单位毫秒。 |
| end_time | Long | 作业结束时间。取值范围:unix时间戳,单位毫秒。 |
| create_user_id | String | 创建作业的用户ID。取值范围:长度为0-256个字符。 |
| create_user_name | String | 创建作业的用户名称。取值范围:长度为0-256个字符。 |
| log_url | String | 日志归档路径OBS URL。取值范围:obs://开头的OBS路径。 |

**ShowSparkJobArtifactResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| job_type | String | 作业类型。取值范围:spark_jar_job(Spark jar作业)、spark_python_job(Python Spark作业)、spark_sql_scripting_job(SQL脚本作业,预留)。 |
| spark_jar_parameter | ShowSparkJarParameterResponse object | Spark Jar作业参数。 |
| spark_py_parameter | ShowSparkPyParameterResponse object | Spark Python作业参数。 |
| spark_sql_scripting_parameter | ShowSparkScriptSQLParameterResponse object | Spark Script SQL作业参数。 |

**ShowSparkJarParameterResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| main_class | String | Spark Jar作业入口类。 |
| main_args | Array of strings | Spark Jar作业入口类参数,多个参数之间空格分隔。取值范围:最多100个参数,每个参数长度为1-512个字符。 |
| main_jar | String | Spark Jar作业主类所在Jar包的OBS路径。取值范围:长度为1-512个字符。 |
| dependency_jars | Array of strings | Spark作业依赖Jar包的OBS路径数组。 |
| dependency_files | Array of strings | Spark作业依赖文件包的OBS路径数组。 |
| dependency_archives | Array of strings | Spark作业依赖archives包的OBS路径数组。 |
| dependency_py_files | Array of strings | Spark作业依赖python包OBS路径数组。 |

**ShowSparkPyParameterResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| main_python_file | String | Python Spark类型作业URL全路径。例如:obs://bucket/pyspark/pySpark_udf_python.py。 |
| main_args | Array of strings | Spark Python作业入口类参数,多个参数之间空格分隔。例如:--output obs://bucket/output/ --input obs://bucket/input/。 |
| dependency_jars | Array of strings | Spark作业依赖Jar包的OBS路径数组。 |
| dependency_files | Array of strings | Spark作业依赖文件包的OBS路径数组。 |
| dependency_archives | Array of strings | Spark作业依赖archives包的OBS路径数组。 |
| dependency_py_files | Array of strings | Spark作业依赖python包OBS路径数组。 |

**ShowSparkScriptSQLParameterResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| sql_scripting_file | String | Spark Script SQL类型作业URL全路径。例如:obs://bucket/sparksqlsciprt/...。 |
| sql_scripting_parameters | Map<String,String> | 作业参数,包含type和externalCatalog。取值范围:Map<string, string>,键值对个数不超过100个。 |
| dependency_jars | Array of strings | Spark作业依赖Jar包的OBS路径数组。 |
| sql_scripting_result_to_obs | Boolean | sql scripting结果是否写入OBS。取值范围:true(表示写入OBS)、false(表示不写入OBS)。 |
| result | SparkSqlScriptingResultResponse object | Spark SQL脚本作业结果。 |

**SparkSqlScriptingResultResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| result_format | String | 查询类Sql结果格式。 |
| result_path | String | 查询类Sql结果OBS路径。 |

**ShowSparkResourceConfigResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| executor_number | Integer | Spark作业的最大executor个数。如果配置,则表示spark.dynamicAllocation.enabled=true,spark.dynamicAllocation.minExecutor=1,spark.dynamicAllocation.maxExecutors=executor_number,spark.dynamicAllocation.initialExecutors=1。 |
| driver_resource_spec | ShowResourceSpecResponse object | 表示对作业使用CPU、内存、磁盘资源的包装,依据本数据结构可以生成对Driver、Executor等资源的粗粒度的描述。 |
| executor_resource_spec | ShowResourceSpecResponse object | 表示对作业使用CPU、内存、磁盘资源的包装,依据本数据结构可以生成对Driver、Executor等资源的粗粒度的描述。 |

**ShowResourceSpecResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| cpu | Integer | CPU核数。取值范围:默认值为1个cpu核数,最低不得小于1个cpu核数。 |
| memory | String | 内存。单位MB,GB。默认GB。默认值为4GB,最低不得小于2GB。 |
| disk | Integer | Spark作业driver、executor的本地磁盘大小。单位GB。 |

**ShowSparkJobImageConfigResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| image_feature | String | 用户作业使用的镜像类型。取值范围:"basic"(基础镜像)、"custom"(自定义镜像)。 |
| image_uri | String | 自定义镜像。当前只支持SWR,格式为:组织名/镜像名:镜像版本。当用户设置"image_feature"为"custom"时该参数生效。取值范围:长度为1-256个字符。 |

**ShowSparkRestoreStrategyResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| max_retry | Integer | 作业失败后最大的重试次数。如果配置的值大于0,则自动开始失败重试;如果不配置或者配置为0,则不开启作业失败重试。取值范围:默认-1(不限制),0-100。 |
| retry_delay | Long | 作业失败重试机制中,每次重试的时间间隔。取值范围:0-600秒,默认30秒。 |
| queued_timeout | Long | 作业提交超时时间,如果超过此时间作业仍未运行,则作业失败。取值范围:10-86400秒(10分钟-1天),默认10800秒(3小时)。 |
| running_timeout | Long | 作业运行超时时间,如果超过此时间作业还未运行结束,则作业会取消运行,并标记失败。取值范围:-1(不限制),10-315360000秒(最小10分钟,最大10年),默认-1。 |

**ShowSparkJobLabelResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| key | String | 标签的键。 |
| value | String | 标签的值。取值范围:长度为1-128个字符。 |

**ShowSparkLoggingConfigResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| driver_root_logger_level | String | Driver根目录日志级别配置。取值范围:"DEBUG"(debug级别)、"TRACE"(trace级别)、"WARN"(warning级别)、"INFO"(info级别)、"ERROR"(error级别)。 |
| driver_loggers_level_of_class | Array of ShowSparkClassLoggerLevelResponse objects | Driver输出日志类的名称和对应的日志级别配置。取值范围:最多20个。 |
| executor_root_logger_level | String | Executor根目录日志级别配置。取值范围:"DEBUG"(debug级别)、"TRACE"(trace级别)、"WARN"(warning级别)、"INFO"(info级别)、"ERROR"(error级别)。 |
| executor_loggers_level_of_class | Array of ShowSparkClassLoggerLevelResponse objects | Executor输出日志类的名称和对应的日志级别配置。取值范围:最多20个。 |

**ShowSparkClassLoggerLevelResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| logger_name | String | 输出日志类的名称。取值范围:长度为1-256个字符。 |
| logger_level | String | 输出日志的级别。取值范围:"DEBUG"(debug级别)、"TRACE"(trace级别)、"WARN"(warning级别)、"INFO"(info级别)、"ERROR"(error级别)。 |

**状态码: 400/401/403**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 200 - 成功**

```json
{
  "job_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e",
  "client_token": "token_value",
  "name": "test_job",
  "endpoint_name": "endpoint1",
  "catalog_name": "catalog1",
  "state": "RUNNING",
  "job_agency": "agency1",
  "job_config": {
    "job_type": "spark_jar_job",
    "spark_jar_parameter": {
      "main_class": "com.example.MainClass",
      "main_args": ["arg1", "arg2"],
      "main_jar": "obs://bucket/jar/main.jar",
      "dependency_jars": ["obs://bucket/jar/dep1.jar"],
      "dependency_files": [],
      "dependency_archives": [],
      "dependency_py_files": []
    }
  },
  "resource_config": {
    "executor_number": 4,
    "driver_resource_spec": {
      "cpu": 2,
      "memory": "4GB",
      "disk": 100
    },
    "executor_resource_spec": {
      "cpu": 2,
      "memory": "4GB",
      "disk": 100
    }
  },
  "image_config": {
    "image_feature": "basic",
    "image_uri": ""
  },
  "restore_strategy": {
    "max_retry": 3,
    "retry_delay": 30,
    "queued_timeout": 10800,
    "running_timeout": -1
  },
  "retry_times": 0,
  "labels": [
    {
      "key": "env",
      "value": "production"
    }
  ],
  "logging_config": {
    "driver_root_logger_level": "INFO",
    "driver_loggers_level_of_class": [],
    "executor_root_logger_level": "INFO",
    "executor_loggers_level_of_class": []
  },
  "description": "Test Spark job",
  "create_time": 1764061598000,
  "start_time": 1764061600000,
  "end_time": null,
  "create_user_id": "user123",
  "create_user_name": "test_user",
  "log_url": "obs://bucket/logs/"
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 1.5 取消 Spark 作业

#### 功能介绍

该API用于取消Spark作业。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkJob:stop | Write | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkJob:stop | Write | sparkJob * | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/spark-jobs/{job_id}/cancel
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| job_id | 是 | String | Spark作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 204**

无响应Body参数。

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 204 - 取消成功**

无响应Body。

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 204 | 取消成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

## 2. SparkSql 作业相关 API

### 2.1 查询 SparkSql 作业状态

#### 功能介绍

该API用于查询SparkSql作业的状态。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:show | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:show | Read | sparkSql * | - | - | - |

#### URI

```
GET https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls/{statement_id}/state
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| statement_id | 是 | String | SparkSql作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| statement_id | String | SparkSql作业的ID,采用UUID(通用唯一识别码)格式。取值范围:UUID格式,长度为36个字符。 |
| state | String | SparkSql作业的状态。取值范围:QUEUED(排队中)、RUNNING(运行中)、CANCELING(取消中)、CANCELED(已取消)、FAILED(已失败)、QUEUED_TIMEOUT(排队超时)、RUNNING_TIMEOUT(运行超时)、SUCCEED(已成功)。 |

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 200 - 成功**

```json
{
  "statement_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e",
  "state": "RUNNING"
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.2 查询 SparkSql 作业列表

#### 功能介绍

该API用于查询Project下SparkSql作业列表。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:list | List | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:list | List | sparkSql * | - | - | - |
| DataArtsFabric:sparkSql:list | List | endpoint * | - | - | - |

#### URI

```
GET https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |

**Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| marker | 否 | String | 作业ID游标位置,用于分页查询。首次查询可不传或传空字符串,后续查询传入上次返回的next_marker值。取值范围:长度为36个字符的字母、数字、中划线及下划线的组合。 |
| limit | 否 | Integer | 查询记录数。取值范围:1-100。默认取值:10。 |
| create_time_after | 否 | Long | 用于查询创建时间在该时间点之后的作业。取值范围:1764061598000-253402271999000。 |
| create_time_before | 否 | Long | 用于查询创建时间在该时间点之前的作业。取值范围:1764061598000-253402271999000。 |
| endpoint_name | 否 | String | 端点名称。取值范围:长度为1-63个字符,只能由小写字母、数字及中划线组成,且必须以字母开头。 |
| statement | 否 | String | Sql片段模糊查询。取值范围:长度为1-128个字符。 |
| statement_id | 否 | String | SparkSql作业ID。取值范围:长度为1-64个字符的字母、数字、中划线及下划线的组合。 |
| states | 否 | Array of strings | SparkSql作业的状态。取值范围:"QUEUED"(排队中)、"RUNNING"(运行中)、"CANCELING"(取消中)、"CANCELED"(已取消)、"FAILED"(已失败)、"QUEUED_TIMEOUT"(排队超时)、"RUNNING_TIMEOUT"(运行超时)、"SUCCEED"(已成功)。 |
| statement_types | 否 | Array of strings | 指定查询的作业类型。当参数statement_types传值不为空时,states参数不生效。取值范围:"DDL"(数据定义语言)、"DCL"(数据控制语言)、"DQL"(数据查询语言)、"DML"(数据操作语言)。 |
| create_user_id | 否 | String | Spark作业创建者。取值范围:长度为1-256个字符。 |
| labels | 否 | String | 指定作业标签作为过滤条件,支持多标签过滤。格式为"key=value",如:k1%3Dv1,"="需要转义为"%3D","k1"为标签键,"v1"为标签值。长度不超过12000个字符。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| page_info | SparkMarkerPageInfo object | 分页游标信息。 |
| statements | Array of ListSparkSqlResItem objects | 详细的SparkSql作业列表。 |

**SparkMarkerPageInfo 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| next_marker | String | 下一页游标,为空表示没有更多数据。取值范围:长度为36个字符的字母、数字、中划线及下划线的组合。 |
| current_count | Integer | 当前页数据条数。取值范围:大于等于0。 |

**ListSparkSqlResItem 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| statement_id | String | SparkSql作业的ID。取值范围:系统生成的唯一标识,长度为36个字符。 |
| client_token | String | SparkSql作业事务ID,防止重复提交。取值范围:UUID格式,长度为36个字符。 |
| endpoint_name | String | 端点名称。取值范围:长度为0-63个字符。 |
| statement_type | String | SQL作业类型。取值范围:DDL(创建修改删除元数据类型的作业、DESC/SHOW等语句)、DCL(权限授权与回收类型的作业)、DQL(查询语句SELECT)、DML(向表追加、删除、更新新数据类型的作业)。 |
| state | String | SparkSql作业的状态。取值范围:QUEUED(排队中)、RUNNING(运行中)、CANCELING(取消中)、CANCELED(已取消)、FAILED(已失败)、QUEUED_TIMEOUT(排队超时)、RUNNING_TIMEOUT(运行超时)、SUCCEED(已成功)。 |
| catalog_context | SparkSqlCatalogContextResponse object | SparkSQL Catalog上下文响应。 |
| statement | String | 用户Sql。取值范围:长度不超过500000个字符。 |
| parameters | Array of SparkSqlParameter objects | 用户SQL内容的占位符的Key/Value。取值范围:最多16个。 |
| metric_statistics | SparkSqlMetricStatisticsResponse object | SparkSQL作业统计指标。 |
| log_url | String | 日志OBS归档路径。取值范围:obs://开头的OBS路径。 |
| create_time | Long | 作业创建时间。取值范围:unix时间戳,单位毫秒。 |
| start_time | Long | 作业开始运行时间。取值范围:unix时间戳,单位毫秒。 |
| end_time | Long | 作业结束时间。取值范围:unix时间戳,单位毫秒。 |
| error | SparkSqlErrorDto object | SparkSql作业错误详情。 |
| create_user_id | String | 创建作业的用户ID。取值范围:长度为0-256个字符。 |
| create_user_name | String | 创建作业的用户名称。取值范围:长度为0-256个字符。 |

**SparkSqlCatalogContextResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| catalog_name | String | Catalog名称。用于指定作业使用的数据目录。取值范围:长度不超过128个字符。 |
| database_name | String | 默认数据库名称。取值范围:长度不超过128个字符。 |

**SparkSqlParameter 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| key | String | 占位符的键。 |
| value | String | 占位符的值。 |

**SparkSqlMetricStatisticsResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| bad_records | Long | 脏数据行数。 |
| input_size | Long | 读取数据字节数。 |
| input_records | Long | 读取数据行数。 |
| output_bytes | Long | 输出数据字节数。 |
| output_records | Long | 输出数据行数。 |
| cpu_time | Long | 计算使用CPU秒数。 |

**SparkSqlErrorDto 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| message | String | SparkSql错误描述。取值范围:长度为0-512个字符。 |
| sql_state | String | SparkSql错误码。取值范围:长度为0-64个字符。 |
| error_class | String | SparkSql错误类型。取值范围:长度为0-128个字符。 |
| line | Integer | 报错行号。 |
| start_position | Integer | 报错起始位置。 |

**状态码: 400/401/403**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 200 - 查看SparkSql作业列表响应body体**

```json
{
  "page_info": {
    "next_marker": "marker_value",
    "current_count": 10
  },
  "statements": [
    {
      "statement_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e",
      "client_token": "token_value",
      "endpoint_name": "endpoint1",
      "statement_type": "DQL",
      "state": "RUNNING",
      "catalog_context": {
        "catalog_name": "catalog1",
        "database_name": "default"
      },
      "statement": "SELECT * FROM table1",
      "parameters": [],
      "metric_statistics": {
        "bad_records": 0,
        "input_size": 1024,
        "input_records": 100,
        "output_bytes": 2048,
        "output_records": 50,
        "cpu_time": 5
      },
      "log_url": "obs://bucket/logs/",
      "create_time": 1764061598000,
      "start_time": 1764061600000,
      "end_time": null,
      "error": null,
      "create_user_id": "user123",
      "create_user_name": "test_user"
    }
  ]
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 查看SparkSql作业列表响应body体 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.3 查看 SparkSql 作业详情

#### 功能介绍

查看SparkSql作业详情。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:show | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:show | Read | sparkSql * | - | - | - |

#### URI

```
GET https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls/{statement_id}
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| statement_id | 是 | String | SparkSql作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| statement_id | String | SparkSql作业的ID。取值范围:系统生成的唯一标识,长度为36个字符。 |
| client_token | String | SparkSql作业事务ID,防止重复提交。取值范围:UUID格式,长度为36个字符。 |
| endpoint_name | String | 端点名称。取值范围:长度为0-63个字符。 |
| state | String | SparkSql作业的状态。取值范围:QUEUED(排队中)、RUNNING(运行中)、CANCELING(取消中)、CANCELED(已取消)、FAILED(已失败)、QUEUED_TIMEOUT(排队超时)、RUNNING_TIMEOUT(运行超时)、SUCCEED(已成功)。 |
| catalog_context | SparkSqlCatalogContextResponse object | SparkSQL Catalog上下文响应。 |
| statement | String | 用户Sql。取值范围:长度不超过500000个字符。 |
| parameters | Array of SparkSqlParameter objects | 用户SQL内容的占位符的Key/Value。取值范围:最多16个。 |
| statement_type | String | SQL作业类型。取值范围:DDL(创建修改删除元数据类型的作业、DESC/SHOW等语句)、DCL(权限授权与回收类型的作业)、DQL(查询语句SELECT)、DML(向表追加、删除、更新新数据类型的作业)。 |
| engine_version | String | 引擎版本。 |
| spark_config | Map<String,String> | 用户自定义Spark参数配置。 |
| log_url | String | 日志OBS归档路径。取值范围:obs://开头的OBS路径。 |
| result | SparkSqlResultResponse object | SparkSQL查询结果。 |
| metric_statistics | SparkSqlMetricStatisticsResponse object | SparkSQL作业统计指标。 |
| timeout | SparkSqlTimeout object | SparkSQL超时配置。 |
| create_time | Long | 作业创建时间。取值范围:unix时间戳,单位毫秒。 |
| start_time | Long | 作业开始运行时间。取值范围:unix时间戳,单位毫秒。 |
| end_time | Long | 作业结束时间。取值范围:unix时间戳,单位毫秒。 |
| create_user_id | String | 创建作业的用户ID。取值范围:长度为0-256个字符。 |
| create_user_name | String | 创建作业的用户名称。取值范围:长度为0-256个字符。 |
| labels | Array of SparkSqlLabelRes objects | 作业标签。取值范围:最多16个。 |
| error | SparkSqlErrorDto object | SparkSql作业错误详情。 |

**SparkSqlResultResponse 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| result_format | String | 查询类Sql结果格式。 |
| result_path | String | 查询类Sql结果OBS路径。 |
| execution_profile_path | String | 作业执行计划的存储路径。 |

**SparkSqlTimeout 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| queued_timeout | Long | Sql提交超时时间,如果超过此时间作业仍未运行,则作业失败。取值范围:10-1440分钟,默认180分钟。 |
| running_timeout | Long | Sql运行超时时间,如果超过此时间作业还未运行结束,则作业会取消运行,并标记为运行超时。取值范围:10-720分钟,默认720分钟。 |

**SparkSqlLabelRes 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| key | String | 标签的键。取值范围:长度为1-128个字符。 |
| value | String | 标签的值。取值范围:长度为1-128个字符。 |

**状态码: 400/401/403**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 200 - 成功**

```json
{
  "statement_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e",
  "client_token": "token_value",
  "endpoint_name": "endpoint1",
  "state": "RUNNING",
  "catalog_context": {
    "catalog_name": "catalog1",
    "database_name": "default"
  },
  "statement": "SELECT * FROM table1",
  "parameters": [],
  "statement_type": "DQL",
  "engine_version": "3.0.0",
  "spark_config": {},
  "log_url": "obs://bucket/logs/",
  "result": {
    "result_format": "json",
    "result_path": "obs://bucket/results/",
    "execution_profile_path": "obs://bucket/profiles/"
  },
  "metric_statistics": {
    "bad_records": 0,
    "input_size": 1024,
    "input_records": 100,
    "output_bytes": 2048,
    "output_records": 50,
    "cpu_time": 5
  },
  "timeout": {
    "queued_timeout": 180,
    "running_timeout": 720
  },
  "create_time": 1764061598000,
  "start_time": 1764061600000,
  "end_time": null,
  "create_user_id": "user123",
  "create_user_name": "test_user",
  "labels": [
    {
      "key": "env",
      "value": "production"
    }
  ],
  "error": null
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.4 取消 SparkSql 作业

#### 功能介绍

该API用于取消SparkSql作业。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:stop | Write | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:stop | Write | sparkSql * | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls/{statement_id}/cancel
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| statement_id | 是 | String | SparkSql作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 204**

无响应Body参数。

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 204 - 取消成功**

无响应Body。

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 204 | 取消成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.5 检查 SparkSql 语法

#### 功能介绍

该API用于检查SparkSql语法。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:check | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:check | Read | sparkSql * | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls/check
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |

**Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| endpoint_name | 是 | String | 端点名称。取值范围:长度为1-63个字符,只能由小写字母、数字及中划线组成,且必须以字母开头。 |
| catalog_context | 否 | SparkSqlCatalogContext object | SparkSQL Catalog上下文配置。 |
| statement | 是 | String | 用户Sql。取值范围:长度不超过500000个字符。 |
| paramaters | 否 | Array of SparkSqlParameter objects | 用户SQL内容的占位符的Key/Value。取值范围:最多16个。 |

**SparkSqlCatalogContext 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| catalog_name | 否 | String | Catalog名称。用于指定作业使用的数据目录。取值范围:长度不超过128个字符。 |
| database_name | 否 | String | 默认数据库名称。取值范围:长度不超过128个字符。 |

**SparkSqlParameter 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| key | 是 | String | 占位符的键。 |
| value | 是 | String | 占位符的值。 |

#### 响应参数

**状态码: 204**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| success | Boolean | SparkSql语法是否校验通过。取值范围:true(表示校验通过)、false(表示校验未通过)。 |
| message | String | SparkSql错误描述。取值范围:长度为0-512个字符。 |
| line | Integer | 报错行号。取值范围:大于等于1。 |
| start_position | Integer | 报错起始位置。取值范围:大于等于0。 |

**状态码: 400/401/403**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 204 - 语法成功**

```json
{
  "success": true,
  "message": "",
  "line": 0,
  "start_position": 0
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 204 | 语法成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.6 执行 SparkSql 作业

#### 功能介绍

该API用于执行SparkSql作业。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:run | Write | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:run | Write | sparkSql * | - | - | - |
| DataArtsFabric:sparkSql:run | Write | endpoint * | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |

**Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| X-Client-Token | 否 | String | 服务事务ID,用于链路追踪。 |

**Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| endpoint_name | 是 | String | 端点名称。取值范围:长度为1-63个字符,只能由小写字母、数字及中划线组成,且必须以字母开头。 |
| catalog_context | 否 | SparkSqlCatalogContext object | SparkSQL Catalog上下文配置。 |
| statement | 是 | String | 用户Sql。取值范围:长度不超过500000个字符。 |
| parameters | 否 | Array of SparkSqlParameter objects | 用户SQL内容的占位符的Key/Value。取值范围:最多16个。 |
| spark_config | 否 | Map<String,String> | 用户自定义Spark参数配置。取值范围:Map<string, string>,键值对个数不超过100个。 |
| timeout | 否 | SparkSqlTimeout object | SparkSQL超时配置。 |
| labels | 否 | Array of SparkSqlLabel objects | 作业标签。取值范围:最多16个。 |

**SparkSqlCatalogContext 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| catalog_name | 否 | String | Catalog名称。用于指定作业使用的数据目录。取值范围:长度不超过128个字符。 |
| database_name | 否 | String | 默认数据库名称。取值范围:长度不超过128个字符。 |

**SparkSqlParameter 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| key | 是 | String | 占位符的键。 |
| value | 是 | String | 占位符的值。 |

**SparkSqlTimeout 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| queued_timeout | 否 | Long | Sql提交超时时间,如果超过此时间作业仍未运行,则作业失败。取值范围:10-1440分钟,默认180分钟。 |
| running_timeout | 否 | Long | Sql运行超时时间,如果超过此时间作业还未运行结束,则作业会取消运行,并标记为运行超时。取值范围:10-720分钟,默认720分钟。 |

**SparkSqlLabel 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| key | 是 | String | 标签的键。 |
| value | 是 | String | 标签的值。 |

#### 响应参数

**状态码: 201**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| statement_id | String | 生成的作业ID。取值范围:系统生成的唯一标识。 |

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 201 - 创建成功**

```json
{
  "statement_id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e"
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 201 | 创建成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.7 预览 SparkSql 作业查询结果

#### 功能介绍

该API用于预览SparkSql作业查询结果。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:sparkSql:show | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| DataArtsFabric:sparkSql:show | Read | sparkSql * | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/spark-sqls/{statement_id}/preview
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| statement_id | 是 | String | SparkSql作业的ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| result_format | String | 查询类Sql结果格式。 |
| result_path | String | 查询类Sql结果OBS路径。 |
| schema | Array of SparkSqlSchemaItem objects | 查询结果的列信息。 |
| rows | Array<Array<String>> | 作业结果集。 |

**SparkSqlSchemaItem 结构**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| column_name | String | 列名。 |
| column_type | String | 列类型。 |

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

无

#### 响应示例

**状态码: 200 - 预览作业成功**

```json
{
  "result_format": "json",
  "result_path": "obs://bucket/results/",
  "schema": [
    {
      "column_name": "id",
      "column_type": "INT"
    },
    {
      "column_name": "name",
      "column_type": "STRING"
    }
  ],
  "rows": [
    ["1", "Alice"],
    ["2", "Bob"]
  ]
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 预览作业成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.8 重启 SparkSql 集群

#### 功能介绍

该API用于重启SparkSql Cluster集群。

**接口约束**

- 重启集群会导致正在运行的作业被终止,请提前做好数据保存
- 不同的operation_type对应不同的重启行为,请谨慎选择

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:endpoint:restart | Write | - | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/endpoints/{endpoint_name}/sql-clusters/restart
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| endpoint_name | 是 | String | 端点名称。取值范围:长度为1-63个字符,只能由小写字母、数字及中划线组成,且必须以字母开头。 |

**Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| restart_strategy | 是 | String | 集群重启策略,用于指定重启方式。取值范围:FORCE(立即终止所有任务并强制重启)、GRACEFUL(等待当前任务完成再重启)。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| operation_id | String | 生成的重启操作ID。取值范围:只有当restart_strategy=FORCE时,返回operation_id;5分钟之内有未完成的重启操作,返回该operation_id。 |

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

**重启SparkSql集群**

```
POST https://endpoint/v2/workspaces/{workspace_id}/endpoints/{endpoint_name}/sql-clusters/restart
```

```json
{
  "endpoint_name": "sql_job_bus",
  "restart_strategy": "FORCE"
}
```

#### 响应示例

**状态码: 200 - 异步下发重启集群成功**

```json
{
  "operation_id": "555-sdsd-sdsd-xxx"
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 异步下发重启集群成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

### 2.9 查询重启 SparkSql 集群状态

#### 功能介绍

该API用于查询重启SparkSql集群状态。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| DataArtsFabric:endpoint:restart | Write | - | - | - | - |

#### URI

```
POST https://localhost.com/v2/workspaces/{workspace_id}/sql-clusters/{operation_id}/restart-state
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 命名空间编号,用于资源隔离。长度为32-36个字符,只能由中文字符、英文字母、数字及中划线组成。 |
| operation_id | 是 | String | SparkSQL集群重启的操作ID。长度为1-64个字符的字母、数字、中划线及下划线的组合。 |

#### 响应参数

**状态码: 200**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| operation_id | String | SparkSQL集群重启的操作ID。取值范围:UUID(通用唯一识别码)格式。 |
| create_time | Long | 重启开始时间。取值范围:unix时间戳,单位毫秒。 |
| end_time | Long | 重启结束时间。取值范围:unix时间戳,单位毫秒。 |
| state | String | SparkSQL集群重启的状态。取值范围:QUEUED(排队中)、FAILED(已失败)、SUCCEED(已成功)。 |
| message | String | SparkSQL集群重启错误描述。取值范围:长度为0-512个字符。 |

**状态码: 400/401/403/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为0-64个字符。 |
| error_msg | String | 错误描述信息。取值范围:长度为0-512个字符。 |
| request_id | String | 内部处理的请求ID。取值范围:长度为0-64个字符。 |

#### 请求示例

**查询重启SparkSql集群状态**

```
POST https://endpoint/v2/workspaces/{workspace_id}/sql-clusters/{operation_id}/restart-state
```

```json
{
  "operation_id": "555-sdsd-sdsd-xxx"
}
```

#### 响应示例

**状态码: 200 - 成功**

```json
{
  "operation_id": "555-sdsd-sdsd-xxx",
  "create_time": 1764061598000,
  "end_time": 1764061650000,
  "state": "SUCCEED",
  "message": ""
}
```

**状态码: 400 - 请求参数错误**

```json
{
  "error_code": "0103.1001",
  "error_msg": "Parameter check errors occur."
}
```

**状态码: 401 - 认证失败**

```json
{
  "error_code": "0103.1033",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - 权限不足**

```json
{
  "error_code": "0103.1031",
  "error_msg": "Permission Denied."
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 500 | 服务器内部错误 |

---

**文档结束**
