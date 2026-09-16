"""
LLM 凭证热重载：credentials_reload.json 读取与应用

背景：Flask 后端的设置界面允许用户在不重启后端的前提下热替换
LLM_API_KEY/LLM_BASE_URL（见 backend/app/services/settings_store.py 的
apply_and_propagate）。但模拟脚本在启动时通过 create_model()/_create_model()
把凭证一次性读进环境变量、构造出的 camel-ai OpenAIModel 又在构造时就把
api_key/base_url 烤进了内部的 OpenAI/AsyncOpenAI 客户端对象里
（camel/models/openai_model.py）——单纯换掉 Flask 进程里的环境变量对
子进程已经创建好的 model 对象完全没有影响。结果是：用户在设置里换了新
Key之后（例如旧Key的额度已经用完），正在跑的这次模拟仍然会用旧Key继续
请求、继续失败，直到整次模拟结束。

解决方式：Flask 侧每次热替换凭证时，都会往
<simulation_dir>/credentials_reload.json 里写入一份"广播"（version 只增
不减，从不删除该文件——见 app/services/simulation_ipc.py 的
write_credentials_reload_broadcast）。twitter/reddit 两个平台各自的主
循环每一轮都廉价地检查一次该文件的mtime，一旦发现 version 比自己上次
应用过的更新，就原地重建各自 model 对象内部的 OpenAI/AsyncOpenAI 客户
端——因为 generate_twitter_agent_graph/generate_reddit_agent_graph 把
同一个 model 对象按引用传给了这个平台的每一个 SocialAgent，原地替换
属性会立刻对所有 agent 生效，不需要重新生成 agent 图。

之所以不能复用 ipc_commands/ 那套命令机制
（SimulationIPCClient.send_command）：那套是单消费者、读完即删
（delete-on-read）。并行模式下 twitter/reddit 是两个独立的协程，如果
凭证更新也走 ipc_commands/，谁先轮询到就会把命令文件删掉，另一个协程
就永远收不到这次更新，静默地继续用旧（可能已欠费）的凭证跑下去。凭证
广播文件必须允许两边各自独立、重复地读取，因此单独用一份"只增不改
语义"的广播文件，而不是塞进 ipc_commands/。

这段代码触达了 camel-ai 的私有属性（_api_key/_url/_client/_async_client），
camel-ai 版本升级可能会改变这些属性名字或构造方式——因此每一处访问都
必须用 hasattr 防御，任何异常都必须"记警告、保留旧 model 继续跑"，绝不能
让这个可选的热重载功能反过来打断一次正在进行中的模拟。

本模块与 run_parallel_simulation.py / run_twitter_simulation.py /
run_reddit_simulation.py 三个模拟脚本一样，是刻意与后端 Flask 应用
解耦的独立模块：不 import 任何 app.* 包（import 它们会连带拉入整个
Flask app 包，例如 `app/services/__init__.py` 会 eager-import 一整套
服务，包括 zep_entity_reader 等重量级 ML 依赖）。

此前这一整套函数在三个模拟脚本里各自维护一份完全相同的拷贝，现在提取
为脚本共享模块，三个脚本改为直接 import 使用。
"""

import os
import json
from typing import Dict, Any, Optional, Tuple


CREDENTIALS_RELOAD_FILENAME = "credentials_reload.json"


def _read_credentials_reload_broadcast(simulation_dir: str) -> Optional[Dict[str, Any]]:
    """宽容读取 credentials_reload.json：文件不存在/为空/损坏/形状不对
    一律返回 None 并打印一条警告，绝不能因为一份格式错误的广播文件而让
    正在运行的模拟崩溃。"""
    path = os.path.join(simulation_dir, CREDENTIALS_RELOAD_FILENAME)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw = f.read()
    except OSError:
        return None

    if not raw.strip():
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  警告: 凭证热重载广播文件解析失败，忽略本次: {path} ({e})")
        return None

    if not isinstance(data, dict) or not isinstance(data.get("version"), int):
        print(f"  警告: 凭证热重载广播文件格式不正确（缺少整数version），忽略本次: {path}")
        return None

    return data


def _mask_secret_for_log(value: Optional[str]) -> str:
    """日志打印用的掩码，绝不在日志/控制台输出完整的API Key。"""
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _apply_credentials_reload(model, data: Dict[str, Any], platform: str) -> bool:
    """把 broadcast 中的新 LLM 凭证原地灌入已经构造好的 camel OpenAIModel
    实例：重建它内部的 _client/_async_client，并更新 _api_key/_url。

    Returns:
        True 表示确实重建了客户端；False 表示跳过（凭证为空、camel-ai
        内部结构与预期不符、或重建过程本身出错）。永远不抛出异常——调用方
        (一个正在运行中的模拟主循环) 不能因为这个可选功能而被打断。
    """
    new_api_key = data.get("llm_api_key")
    new_base_url = data.get("llm_base_url")

    if not new_api_key:
        print(f"  警告: [{platform}] 凭证热重载广播中缺少有效的llm_api_key，跳过本次重载")
        return False

    required_attrs = ("_client", "_async_client", "_api_key", "_url")
    missing = [attr for attr in required_attrs if not hasattr(model, attr)]
    if missing:
        print(
            f"  警告: [{platform}] 当前camel-ai版本的模型对象缺少预期属性 {missing}，"
            f"无法执行凭证热重载（camel-ai内部实现可能已发生变化），继续使用旧凭证运行"
        )
        return False

    try:
        client_cls = type(model._client)
        async_client_cls = type(model._async_client)
        timeout = getattr(model, "_timeout", None)
        max_retries = getattr(model, "_max_retries", 3)

        new_client = client_cls(
            timeout=timeout,
            max_retries=max_retries,
            base_url=new_base_url,
            api_key=new_api_key,
        )
        new_async_client = async_client_cls(
            timeout=timeout,
            max_retries=max_retries,
            base_url=new_base_url,
            api_key=new_api_key,
        )
    except Exception as e:
        print(f"  警告: [{platform}] 重建OpenAI客户端失败，继续使用旧凭证运行: {e}")
        return False

    # 两个新客户端都构造成功之后才整体替换，避免中途失败导致model处于
    # "一半新一半旧"的不一致状态。
    model._client = new_client
    model._async_client = new_async_client
    model._api_key = new_api_key
    model._url = new_base_url

    print(
        f"  [{platform}] 已应用LLM凭证热重载: version={data.get('version')}, "
        f"api_key={_mask_secret_for_log(new_api_key)}, base_url={new_base_url or '默认'}"
    )
    return True


def _poll_and_apply_credentials_reload(
    simulation_dir: str,
    model,
    last_mtime: Optional[float],
    last_version: int,
    platform: str,
) -> Tuple[Optional[float], int]:
    """每轮廉价地检查一次credentials_reload.json是否有新版本：只有当文件
    mtime发生变化时才真正打开+解析JSON并比较version，避免每轮都做一次
    完整的文件IO。

    Returns:
        更新后的 (last_mtime, last_version)；调用方需要把它们原样保存
        下来供下一轮使用。stale/相等的version不会被重复应用；格式错误的
        广播文件会被忽略（只打一条警告），既不重试也不崩溃。
    """
    path = os.path.join(simulation_dir, CREDENTIALS_RELOAD_FILENAME)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return last_mtime, last_version

    if mtime == last_mtime:
        return last_mtime, last_version

    data = _read_credentials_reload_broadcast(simulation_dir)
    if data is None:
        return mtime, last_version

    version = data["version"]
    if version <= last_version:
        return mtime, last_version

    _apply_credentials_reload(model, data, platform)
    return mtime, version
