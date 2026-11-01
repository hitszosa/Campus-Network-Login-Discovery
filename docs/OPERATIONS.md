# 运行与部署

项目提供 `cnld-agent`（HTTP 探测与 MQTT 上报）和 `cnld-watch`（订阅并显示当前状态）。默认实现独立运行，不需要 Kubernetes。自动登录、独立外网探测与 K8s sink 留在后续版本。

## 本机跑通

需要 Go 1.24 或更新版本，以及 Mosquitto。Ubuntu/Debian 安装 Broker 和客户端：

```sh
sudo apt-get install mosquitto mosquitto-clients
sudo systemctl stop mosquitto
make build
```

Arch Linux 可用 `sudo pacman -S mosquitto`。依次在四个终端运行：

```sh
# 终端 1
mosquitto -c deploy/mosquitto-lab.conf -v
```

```sh
# 终端 2
printf yes > /tmp/cnld-mode
go run ./cmd/fake-campus
```

```sh
# 终端 3
./bin/cnld-watch -node lab-01
```

```sh
# 终端 4
./bin/cnld-agent -config configs/lab.yaml
```

再开一个终端执行 `printf no > /tmp/cnld-mode`，订阅工具在下一轮显示 `auth=false`。可选模式：`yes`、`no`、`portal`、`redirect`、`slow`、`large`；最后三种得到 unknown。Agent 每轮都上报，状态变化时完成本轮判断后立即投递。

按 Ctrl+C 正常退出 Agent，订阅工具显示 offline；强杀则由 Broker 发现断连后发布遗嘱。`fake-campus` 和 `fake-link` 是实验工具，实际部署只运行 Agent。

## 接入真实校园网

```sh
cp configs/agent.example.yaml configs/local.yaml
```

编辑 `configs/local.yaml`，填入实际 endpoint、响应规则、唯一的 `node_id` 和 MQTT Broker。用 `curl -i` 分别采集已认证与未认证响应，再设置 `status_code`、`body_equals` 和 `portal_hosts`。body 做完整比较，末尾换行也计入内容；例如 `body_equals: "authenticated\n"`。

默认 Client ID 为 `cnld-<node_id>`。同一 node_id 只运行一份 Agent；复制配置到第二台机器时先修改 node_id。`client_id` 和 `topic_prefix` 可以显式覆盖。

```sh
export CNLD_MQTT_USERNAME=agent-worker-01
read -r -s CNLD_MQTT_PASSWORD
export CNLD_MQTT_PASSWORD
./bin/cnld-agent -config configs/local.yaml
```

单次探测并输出 JSON：

```sh
./bin/cnld-agent probe -config configs/local.yaml
```

`probe` 不连接 Broker，也不要求 MQTT 凭证已设置。完成采样后退出 0，配置错误退出 1；认证结果读取 JSON 的 `labels`。超时等采集失败仍是一份 `unknown` 结果。

配置时间约定：`0 < timeout < interval`，`valid_for > interval + timeout`，有效期为整秒且最多一天。Keep Alive 为整秒且大于零。启动时拒绝未知 YAML 字段、多个 YAML 文档、歧义规则和未支持的 sink。

## TLS 与 Topic 权限

`broker: tls://mqtt.example.edu:8883` 默认校验证书和主机名。公共 CA 使用系统信任库，私有 CA 配置 `mqtt.ca_file`。需要客户端证书时同时配置 `cert_file` 和 `key_file`。文件路径相对进程工作目录；服务部署建议写绝对路径。

Broker 可参考 [TLS 配置](../deploy/mosquitto-tls.example.conf) 和 [ACL](../deploy/mosquitto-acl.example)。修改 ACL 中的节点 ID 与用户名，使 Agent 只能向自己的 report 与 availability 发布，包含遗嘱发布权限。观察者使用单独的只读账户。

```sh
sudo mosquitto_passwd -c /etc/mosquitto/passwd agent-worker-01
sudo mosquitto_passwd /etc/mosquitto/passwd observer
sudo install -m 644 deploy/mosquitto-acl.example /etc/mosquitto/cnld-acl
```

提供实际服务端证书与密钥，按发行版 Mosquitto 的配置目录加载示例并重启 Broker。实验 CA 的生成可参考[指导书第 30 节](../IMPLEMENTATION_GUIDE.md#30--让本机实验先跑通-tls约-10-分钟)。Broker 最好位于未认证节点仍可访问的网络。

使用只读账户订阅：

```sh
export CNLD_MQTT_USERNAME=observer
read -r -s CNLD_MQTT_PASSWORD
export CNLD_MQTT_PASSWORD
./bin/cnld-watch -broker tls://mqtt.example.edu:8883 -node worker-01 \
  -ca-file /etc/cnld/ca.crt
# Broker 要求 mTLS 时，再添加 -cert-file ... -key-file ...
```

直接观察原始报文也可以使用 `mosquitto_sub`，但要将 availability、connection_id、sequence 与有效期一起解释。retained 表示最后一份数据，不代表仍有效。

| 数据 | 当前视图 |
| --- | --- |
| 消费者与 Broker 失联 | unknown |
| availability 明确 offline | offline，认证标签仅作为历史 |
| online，但报告缺失、过期或连接 ID 不匹配 | unknown |
| online、同一连接、最新序号且报告未过期 | 使用报告中的标签，标签本身也可能为 unknown |

QoS 1 可能重复投递；同一连接中旧序号和重复序号不会覆盖新结果。Agent 与消费者要同步时钟。Broker 开启并验证 retained 持久化；Broker 崩溃后的保留数据取决于落盘策略，恢复后仍检查时间和连接 ID。MQTT 3.1.1 的正常 DISCONNECT 不触发遗嘱，遗嘱里不记录未来真实断线时间。

## Linux systemd

```sh
make build
id cnld >/dev/null 2>&1 || sudo useradd --system --user-group --no-create-home cnld
sudo install -m 755 bin/cnld-agent /usr/local/bin/cnld-agent
sudo install -d -m 755 /etc/cnld
sudo install -m 640 -o root -g cnld configs/local.yaml /etc/cnld/config.yaml
sudo install -m 644 deploy/cnld-agent.service /etc/systemd/system/cnld-agent.service
```

配置中的 CA、客户端证书及私钥路径改为 `/etc/cnld/...`。私钥可设置 `root:cnld` 所有权和 `640` 权限。凭证写入 `/etc/cnld/credentials.env`，内容为两行：

```text
CNLD_MQTT_USERNAME=agent-worker-01
CNLD_MQTT_PASSWORD=实际密码
```

```sh
sudo chmod 600 /etc/cnld/credentials.env
sudo systemctl daemon-reload
sudo systemctl enable --now cnld-agent
sudo journalctl -u cnld-agent -f
# 停止时应发布 graceful_shutdown
sudo systemctl stop cnld-agent
```

服务默认使用普通 `cnld` 用户、只读系统目录和独立临时目录。`TimeoutStopSec` 应大于 MQTT `publish_timeout` 并留出清理时间；示例 15 秒适合默认 5 秒发布超时。

## OCI 容器

```sh
make image
docker run --rm --name cnld --network host \
  -v "$PWD/configs/lab.yaml:/config.yaml:ro" cnld:local
```

Linux 的 host networking 让探测使用宿主机出口。上面实验配置连接宿主机 18080 与 1883；实际部署挂载真实配置。TLS CA 也需挂载进容器，配置路径使用容器内路径。例如将 CA 挂载到 `/ca.crt` 并配置 `ca_file: /ca.crt`。

镜像默认 UID/GID 为 `65532:65532`，挂载文件需可供该用户读取。客户端私钥为当前用户的 `600` 文件时，可以显式使用 `--user "$(id -u):$(id -g)"` 运行。凭证通过 `--env-file /绝对路径/credentials.env` 注入。

```sh
docker stop cnld
```

镜像使用 scratch、静态 Go 二进制和系统 CA 集合。构建阶段固定 Go 镜像版本与 digest；用 `--build-arg GO_IMAGE=...` 可显式更新。普通 Linux 二进制和容器都需要验证真实探测源 IP 与目标节点一致，HTTP 执行器不继承环境 HTTP 代理。

## 构建产物与验证

```sh
make test
make integration  # 本机安装 mosquitto 后运行
make dist
```

`bin/` 生成 Linux amd64/arm64 的 Agent、订阅工具及 `SHA256SUMS`。`VERSION=自定义版本 make dist` 设置 Agent 的 `-version` 输出。

[Build and deployment Actions](https://github.com/hitszosa/Campus-Network-Login-Discovery/actions/workflows/build.yml) 的成功运行提供可下载的 `cnld-linux-binaries` 与 `cnld-linux-oci` artifacts，有效期 14 天。下载二进制后执行：

```sh
sha256sum -c SHA256SUMS
chmod +x cnld-agent-linux-amd64 cnld-watch-linux-amd64
./cnld-agent-linux-amd64 -version
```

OCI artifact 包含双架构 `cnld-images.oci.tar`、用于普通 Docker 导入的 amd64 镜像和校验和：

```sh
sha256sum -c OCI-SHA256SUMS
docker load -i cnld-image-amd64.docker.tar.gz
docker run --rm --network host -v "$PWD/configs/lab.yaml:/config.yaml:ro" cnld:ci
```

也可自行导出双架构 OCI：

```sh
docker buildx build --platform linux/amd64,linux/arm64 --provenance=false \
  --output type=oci,dest=bin/cnld-images.oci.tar .
python3 scripts/check-oci.py bin/cnld-images.oci.tar
```

CI 包含 Go 1.24/stable、race、vet、真实 Broker 故障/TLS/ACL 测试，以及 Docker、systemd 的启动上报和正常退出验证。集成测试使用临时端口、测试证书和专用数据目录；Ubuntu 26.04 的 AppArmor 环境在 CI 中仅放行 `/tmp/cnld-mqtt-ci/` 供测试 Broker 使用，保留其他规则。本机出现 AppArmor 配置访问拒绝时，按发行版规则放行自己的实验目录或使用标准 Broker 配置目录。

K8s sink、自动登录、独立外网探测和 metrics 保留为可选后续能力。
