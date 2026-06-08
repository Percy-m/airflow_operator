# Ray API 文档

文档版本: 01  
发布日期: 2026-05-20

---

## 目录

### 1. RayJob 管理

- [1.1 运行作业](#11-运行作业)
- [1.2 查询 Ray 作业列表](#12-查询-ray-作业列表)
- [1.3 查询作业详情](#13-查询作业详情)
- [1.4 取消作业运行](#14-取消作业运行)

---

## 1. RayJob 管理

### 1.1 运行作业

#### 功能介绍

运行作业。支持Ray作业,接口返回作业运行ID。此接口为同步接口,无配套使用接口和特殊场景。

#### 调用方法

请参见如何调用API。

#### 授权信息

账号具备所有API的调用权限,如果使用账号下的IAM用户调用当前API,该IAM用户需具备调用API所需的权限。

- 如果使用角色与策略授权,具体权限要求请参见权限和授权项。
- 如果使用身份策略授权,需具备如下身份策略权限。

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| AIDataLake:rayJob:runJobInstance | Write | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| AIDataLake:rayJob:runJobInstance | Write | rayJob * | - | - | - |

#### URI

```
POST /v2/workspaces/{workspace_id}/ray-jobs
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 工作空间ID。获取方法,请参见获取工作空间ID。取值范围:长度为[1,36]的英文字符、数字和中划线(-)的组合。 |

**Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| X-Auth-Token | 否 | String | 租户Token。用于调用接口的认证。获取方法,请参见认证鉴权。取值范围:长度不超过65534个字符。 |
| X-Transaction-ID | 否 | String | 作业事务ID,防止重复提交。长度限制:1-64个字符。取值范围:长度不超过64个字符。 |

**Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| name | 是 | String | Job名称。取值范围:长度为[1,64]的中文、字母、数字、下划线、中划线、半角句号(.)、空格的组合。 |
| description | 否 | String | 描述信息。取值范围:[0,1024]。 |
| config | 是 | RayJobConfig object | Ray作业类型的配置信息。 |
| endpoint_name | 是 | String | 端点名称。长度限制:1-128个字符。包含中文、字母、数字、下划线、中划线、半角句号(.)、空格的组合。 |
| labels | 否 | Array of JobLabel objects | 标签。 |

**RayJobConfig 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| entrypoint | 是 | String | 运行作业主脚本。取值范围:[1,102400]。 |
| runtime_env | 否 | RuntimeEnv object | 作业的运行时环境配置。可选参数有:working_dir(代码将在其中运行的工作目录,必须是远程URI,如s3或git路径)、py_modules(将与运行时环境一起安装的Python模块,这些必须是远程URI)、pip(要安装的pip软件包列表)、conda(conda YAML配置或本地conda env的名称,例如"pytorch_p36")、env_vars(要设置的环境变量)。 |

**RuntimeEnv 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| working_dir | 否 | String | 代码将在其中运行的工作目录。必须是远程URI,如s3或git路径。取值范围:最小长度1,最大长度2048。 |
| py_modules | 否 | Array of strings | 将与运行时环境一起安装的Python模块。必须是远程URI。取值范围:最小个数0,最大个数1023。 |
| pip | 否 | Array of strings | 要安装的pip软件包列表。取值范围:最小个数0,最大个数1023。 |
| env_vars | 否 | Map<String,String> | 要设置的环境变量。 |
| config | 否 | String | 运行环境的配置。 |

**JobLabel 结构**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| key | 是 | String | 标签的键。 |
| value | 是 | String | 标签的值。 |

#### 响应参数

**状态码: 202**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| id | String | 作业ID。取值范围:长度为[1,64]的中文、字母、数字、下划线、中划线、半角句号(.)、空格的组合。 |
| version_id | String | 作业版本ID。取值范围:长度为[1,36]的英文字符、数字和中划线(-)的组合。 |

**状态码: 400/401/404/408/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为[8,36]个字符。 |
| error_msg | String | 错误描述。取值范围:长度为[2,512]个字符。 |
| solution_msg | String | 解决方案描述。取值范围:长度为[0,4096]个字符。 |

#### 请求示例

**运行Ray作业**

```
POST https://{endpoint}/v2/workspaces/{workspace_id}/ray-jobs
```

```json
{
  "name": "name",
  "description": "description",
  "endpoint_name": "ray",
  "config": {
    "entrypoint": "python hello.py",
    "runtime_env": {
      "working_dir": "obs://bucket/dir1/",
      "py_modules": ["string"],
      "pip": ["numpy=1.16.1"],
      "env_vars": {
        "additionalProp1": "value1",
        "additionalProp2": "value2",
        "additionalProp3": "value3"
      }
    }
  }
}
```

#### 响应示例

**状态码: 202 - 创建Ray作业的响应体**

```json
{
  "id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e"
}
```

**状态码: 400 - BadRequest**

```json
{
  "error_code": "common.01000001",
  "error_msg": "failed to read http request, please check your input, code: 400, reason: Type mismatch., cause: TypeMismatchException"
}
```

**状态码: 401 - Unauthorized**

```json
{
  "error_code": "APIG.1002",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - Forbidden**

```json
{
  "error": {
    "code": "403",
    "message": "X-Auth-Token is invalid in the request",
    "title": "Forbidden"
  },
  "error_code": 403,
  "error_msg": "X-Auth-Token is invalid in the request",
  "title": "Forbidden"
}
```

**状态码: 404 - NotFound**

```json
{
  "error_code": "common.01000001",
  "error_msg": "response status exception, code: 404"
}
```

**状态码: 408 - Request Time-out**

```json
{
  "error_code": "common.00000408",
  "error_msg": "timeout exception occurred"
}
```

**状态码: 500 - InternalServerError**

```json
{
  "error_code": "common.00000500",
  "error_msg": "internal error"
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 202 | 创建Ray作业的响应体 |
| 400 | BadRequest |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | NotFound |
| 408 | Request Time-out |
| 500 | InternalServerError |

---

### 1.2 查询 Ray 作业列表

#### 功能介绍

列举运行的Ray作业。列举工作空间下的作业,分页返回。支持按作业名称、id、端点等信息查询,接口分页返回作业列表。此接口为同步接口,无配套使用接口和特殊场景。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| AIDataLake:rayJob:listJobInstance | List | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| AIDataLake:rayJob:listJobInstance | List | rayJob * | - | - | - |

#### URI

```
GET /v2/workspaces/{workspace_id}/ray-jobs
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 工作空间ID。获取方法,请参见获取工作空间ID。取值范围:长度为[1,36]的英文字符、数字和中划线(-)的组合。 |

**Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| marker | 否 | String | 上一页中最后一条记录id,查询第一页时传空值。取值范围:长度为[1,36]的字母、数字、中划线(-)的组合。 |
| limit | 否 | Integer | 指定每一页返回的最大条目数。取值范围:[1,100]。默认取值:10。 |
| name | 否 | String | 通过名字搜索作业。 |
| id | 否 | String | 通过作业id检索。 |
| endpoint_name | 否 | String | 通过端点名称检索的参数。取值范围:长度为[32,36]的英文字符、数字和中划线(-)的组合。 |
| labels | 否 | String | 指定作业标签作为过滤条件,支持多标签过滤。格式为"key=value",如:GET /v2/workspaces/{workspace_id}/ray-jobs?labels=k1%3Dv1,"="需要转义为"%3D","k1"为标签键,"v1"为标签值。 |

**Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| X-Auth-Token | 否 | String | 租户Token。用于调用接口的认证。获取方法,请参见认证鉴权。取值范围:长度不超过65534个字符。 |

#### 响应参数

**状态码: 200**

查询RayJob列表的响应体。

**状态码: 400/401/404/408/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为[8,36]个字符。 |
| error_msg | String | 错误描述。取值范围:长度为[2,512]个字符。 |
| solution_msg | String | 解决方案描述。取值范围:长度为[0,4096]个字符。 |

#### 请求示例

```
GET https://{endpoint}/v2/workspaces/{workspace_id}/ray-jobs
```

#### 响应示例

**状态码: 200 - 查询RayJob列表的响应体**

```json
null
```

**状态码: 400 - BadRequest**

```json
{
  "error_code": "common.01000001",
  "error_msg": "failed to read http request, please check your input, code: 400, reason: Type mismatch., cause: TypeMismatchException"
}
```

**状态码: 401 - Unauthorized**

```json
{
  "error_code": "APIG.1002",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - Forbidden**

```json
{
  "error": {
    "code": "403",
    "message": "X-Auth-Token is invalid in the request",
    "title": "Forbidden"
  },
  "error_code": 403,
  "error_msg": "X-Auth-Token is invalid in the request",
  "title": "Forbidden"
}
```

**状态码: 404 - NotFound**

```json
{
  "error_code": "common.01000001",
  "error_msg": "response status exception, code: 404"
}
```

**状态码: 408 - Request Time-out**

```json
{
  "error_code": "common.00000408",
  "error_msg": "timeout exception occurred"
}
```

**状态码: 500 - InternalServerError**

```json
{
  "error_code": "common.00000500",
  "error_msg": "internal error"
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 查询RayJob列表的响应体 |
| 400 | BadRequest |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | NotFound |
| 408 | Request Time-out |
| 500 | InternalServerError |

---

### 1.3 查询作业详情

#### 功能介绍

查看Ray作业详情。

#### 调用方法

请参见如何调用API。

#### 授权信息

| 授权项 | 访问级别 | 资源类型 | 条件键 | 别名 | 依赖的授权项 |
|--------|----------|----------|--------|------|--------------|
| AIDataLake:rayJob:showJobInstance | Read | workspace * | g:ResourceTag/<tag-key>, g:EnterpriseProjectId | - | - |
| AIDataLake:rayJob:showJobInstance | Read | rayJob * | - | - | - |

#### URI

```
GET /v2/workspaces/{workspace_id}/ray-jobs/{job_id}
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 工作空间ID。获取方法,请参见获取工作空间ID。取值范围:长度为[1,36]的英文字符、数字和中划线(-)的组合。 |
| job_id | 是 | String | 作业ID。 |

**Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| X-Auth-Token | 否 | String | 租户Token。用于调用接口的认证。获取方法,请参见认证鉴权。取值范围:长度不超过65534个字符。 |

#### 响应参数

**状态码: 200**

查询作业详情。

**状态码: 400/401/404/408/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为[8,36]个字符。 |
| error_msg | String | 错误描述。取值范围:长度为[2,512]个字符。 |
| solution_msg | String | 解决方案描述。取值范围:长度为[0,4096]个字符。 |

#### 请求示例

```
GET https://{endpoint}/v2/workspaces/{workspace_id}/ray-jobs/{job_id}
```

```json
{
  "id": "id",
  "name": "name",
  "description": "description",
  "endpoint_name": "rayclusterendpoint",
  "config": {
    "entrypoint": "python hello.py",
    "runtime_env": {
      "working_dir": "obs://bucket/dir/",
      "py_modules": ["string"],
      "pip": ["numpy=1.16.1", "pandas==0.24.2"],
      "env_vars": {
        "additionalProp1": "value1",
        "additionalProp2": "value2",
        "additionalProp3": "value3"
      },
      "config": "string"
    }
  }
}
```

#### 响应示例

**状态码: 200 - 查询作业详情**

```json
null
```

**状态码: 400 - BadRequest**

```json
{
  "error_code": "common.01000001",
  "error_msg": "failed to read http request, please check your input, code: 400, reason: Type mismatch., cause: TypeMismatchException"
}
```

**状态码: 401 - Unauthorized**

```json
{
  "error_code": "APIG.1002",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - Forbidden**

```json
{
  "error": {
    "code": "403",
    "message": "X-Auth-Token is invalid in the request",
    "title": "Forbidden"
  },
  "error_code": 403,
  "error_msg": "X-Auth-Token is invalid in the request",
  "title": "Forbidden"
}
```

**状态码: 404 - NotFound**

```json
{
  "error_code": "common.01000001",
  "error_msg": "response status exception, code: 404"
}
```

**状态码: 408 - Request Time-out**

```json
{
  "error_code": "common.00000408",
  "error_msg": "timeout exception occurred"
}
```

**状态码: 500 - InternalServerError**

```json
{
  "error_code": "common.00000500",
  "error_msg": "internal error"
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 查询作业详情 |
| 400 | BadRequest |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | NotFound |
| 408 | Request Time-out |
| 500 | InternalServerError |

---

### 1.4 取消作业运行

#### 功能介绍

取消作业运行。主要在取消运行Ray job等job场景使用;输入workspace_id和job_id;输出为接口运行成功或失败的响应消息,无具体的返回值内容。此接口为同步接口,无配套使用接口和特殊场景。

#### 调用方法

请参见如何调用API。

#### 授权信息

账号具备所有API的调用权限,如果使用账号下的IAM用户调用当前API,该IAM用户需具备调用API所需的权限。

- 如果使用角色与策略授权,具体权限要求请参见权限和授权项。
- 如果使用身份策略授权,当前API调用无需身份策略权限。

#### URI

```
POST /v2/workspaces/{workspace_id}/ray-jobs/{job_id}/cancel
```

#### 请求参数

**路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| workspace_id | 是 | String | 工作空间ID。获取方法,请参见获取工作空间ID。取值范围:长度为[1,36]的英文字符、数字和中划线(-)的组合。 |
| job_id | 是 | String | 作业ID。 |

**Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
|------|----------|----------|------|
| X-Auth-Token | 否 | String | 租户Token。用于调用接口的认证。获取方法,请参见认证鉴权。取值范围:长度不超过65534个字符。 |

#### 响应参数

**状态码: 202**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| id | String | 作业ID。取值范围:长度为[1,64]的中文、字母、数字、下划线、中划线、半角句号(.)、空格的组合。 |
| version_id | String | 作业版本ID。取值范围:长度为[1,36]的英文字符、数字和中划线(-)的组合。 |

**状态码: 400/401/404/408/500**

| 参数 | 参数类型 | 描述 |
|------|----------|------|
| error_code | String | 错误码。取值范围:长度为[8,36]个字符。 |
| error_msg | String | 错误描述。取值范围:长度为[2,512]个字符。 |
| solution_msg | String | 解决方案描述。取值范围:长度为[0,4096]个字符。 |

#### 请求示例

**取消作业运行,无请求体**

```
POST https://{endpoint}/v2/workspaces/{workspace_id}/ray-jobs/{job_id}/cancel
```

#### 响应示例

**状态码: 202 - 取消Ray作业的响应体**

```json
{
  "id": "ac8111bf-3601-4905-8ddd-b41d3e636a4e"
}
```

**状态码: 400 - BadRequest**

```json
{
  "error_code": "common.01000001",
  "error_msg": "failed to read http request, please check your input, code: 400, reason: Type mismatch., cause: TypeMismatchException"
}
```

**状态码: 401 - Unauthorized**

```json
{
  "error_code": "APIG.1002",
  "error_msg": "Incorrect token or token resolution failed"
}
```

**状态码: 403 - Forbidden**

```json
{
  "error": {
    "code": "403",
    "message": "X-Auth-Token is invalid in the request",
    "title": "Forbidden"
  },
  "error_code": 403,
  "error_msg": "X-Auth-Token is invalid in the request",
  "title": "Forbidden"
}
```

**状态码: 404 - NotFound**

```json
{
  "error_code": "common.01000001",
  "error_msg": "response status exception, code: 404"
}
```

**状态码: 408 - Request Time-out**

```json
{
  "error_code": "common.00000408",
  "error_msg": "timeout exception occurred"
}
```

**状态码: 500 - InternalServerError**

```json
{
  "error_code": "common.00000500",
  "error_msg": "internal error"
}
```

#### 状态码

| 状态码 | 描述 |
|--------|------|
| 202 | 取消Ray作业的响应体 |
| 400 | BadRequest |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | NotFound |
| 408 | Request Time-out |
| 500 | InternalServerError |

---

**文档结束**
