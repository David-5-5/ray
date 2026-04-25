# Bazel 版本不匹配问题修复记录

## 问题现象

**耗时：3 小时**

每次重启电脑/新开终端，进入 `ray-dev` conda 环境后：
```bash
bazel --version
# 显示 bazel 5.4.0（不兼容，编译报错）

# 但系统明明安装了 6.5.0
/usr/local/bin/bazel --version
# 显示 bazel 6.5.0（正确版本）
```

---

## 根本原因

### ⚠️ 隐藏陷阱：手动覆盖会失效！

**现象**：
```bash
# 临时覆盖 conda 内的 bazel
cp /usr/local/bin/bazel /home/luming/miniconda3/envs/ray-dev/bin/bazel
bazel --version   # 6.5.0 ✓ 暂时成功

# 重新进入 conda 环境
conda deactivate
conda activate ray-dev
bazel --version   # ❌ 又变回 5.4.0！
```

---

### 真正的根本原因：Conda 包的硬链接机制

**真相**：`bazel 5.4.0` 不是普通文件，它是一个 **conda 包**：
```bash
conda list -n ray-dev bazel
# bazel  5.4.0  h12e2e3f_0  conda-forge
```

**文件分布**：
| 位置 | 说明 |
|------|------|
| `~/miniconda3/pkgs/bazel-5.4.0-*/bin/bazel` | 📦 **包缓存中的原始文件（真正常驻）** |
| `~/miniconda3/envs/ray-dev/bin/bazel` | 🔗 **硬链接，指向上面的缓存文件** |

**为什么 `cp` 覆盖会失败？**
1. 你 `cp` 覆盖的只是硬链接文件（环境目录内的）
2. **包缓存中的 5.4.0 原始文件从未被修改**
3. conda 某些操作（激活校验、安装其他包、repair 命令）发现硬链接被篡改
4. conda 自动从缓存重新建立硬链接 → **5.4.0 恢复了**

**为什么 `rm` 删除是永久解决？**
1. `rm` 删除了环境内的硬链接文件
2. conda 不会自动"恢复"已删除的文件（删除是合法的包状态）
3. PATH 找不到 conda 内的 bazel，自动 fallback 到 `/usr/local/bin/bazel`
4. **包缓存中的 5.4.0 还在，但永远不会被调用了**

---

### PATH 优先级问题

| 路径 | Bazel 版本 | 优先级 |
|------|-----------|--------|
| `/home/luming/miniconda3/envs/ray-dev/bin/bazel` | 5.4.0 | 🔴 **更高** |
| `/usr/local/bin/bazel` | 6.5.0 | 🟡 更低 |

### Conda 激活的行为

每次 `conda activate ray-dev` 时，conda 会把环境的 `bin` 目录加到 **PATH 最前面**：
```bash
PATH="/home/luming/miniconda3/envs/ray-dev/bin:$PATH"
```

导致系统 `/usr/local/bin/bazel` 被 conda 环境内的旧版本覆盖。

---

## 解决方案

### ✅ 方案：删除 conda 环境内的 bazel（永久解决）

```bash
# ✅ 删除环境内的 bazel 硬链接（推荐，真正永久解决）
rm -f /home/luming/miniconda3/envs/ray-dev/bin/bazel

# 验证（必须重新进入环境）
conda deactivate
conda activate ray-dev
which bazel          # /usr/local/bin/bazel
bazel --version      # 6.5.0 ✓
```

### 其他可选方案（不推荐）

**方案 2：alias 方式**
在 `~/.bashrc` 或 `~/.zshrc` 加：
```bash
alias bazel='/usr/local/bin/bazel'
```

**方案 3：调整 PATH 顺序**
```bash
# 在 ~/.bashrc 中确保 /usr/local/bin 在前
export PATH="/usr/local/bin:$PATH"
```

---

## Ray Bazel 版本硬性要求

**关键文件**：`WORKSPACE` 第 45-48 行

```python
# Ray 2.50.0 要求必须精确等于 6.5.0
versions.check(
    maximum_bazel_version = "6.5.0",
    minimum_bazel_version = "6.5.0",
)
```

### 典型错误信息

使用 5.4.0 时出现的诡异错误：
```
ERROR: at index 0 of toolchains, got element of type Label, want string
```

这个错误信息非常有误导性，会让人以为是 BUILD 文件语法问题，实际上只是 Bazel 版本不匹配。

---

## 快速诊断命令

遇到编译问题时，先运行这两条命令：

```bash
which bazel          # 检查从哪个路径调用
bazel --version      # 检查版本
```

**必须输出：**
```
/usr/local/bin/bazel
bazel 6.5.0
```

如果不是，说明路径或版本有问题。

---

## 经验总结

1. **Conda 环境内的软件包优先级高于系统**
   - conda 环境激活时，`$CONDA_PREFIX/bin` 会被加到 PATH 最前面
   - 导致同名的系统软件被屏蔽

2. **Ray 的 Bazel 版本要求极其严格**
   - `WORKSPACE` 强制要求版本等于 6.5.0，不能高也不能低
   - 错误信息具有误导性，浪费大量时间

3. **Conda 包的硬链接机制是关键**
   - `bazel 5.4.0` 是 conda 包，不是普通文件
   - 环境内的文件是 **硬链接**，真正的原始文件在 `pkgs/` 包缓存中
   - ❌ `cp` 覆盖只是改了硬链接，缓存的原始文件不变 ——  conda 会自动恢复
   - ✅ `rm` 删除硬链接 —— 永远不会恢复，PATH 自动 fallback 到系统 bazel

4. **验证必须完整**
   - 只看 `bazel --version` 不够
   - 必须看 `which bazel` 确认路径
   - 必须 **重新激活 conda 环境** 后再次验证

---

## 相关文件

| 文件 | 作用 |
|------|------|
| `WORKSPACE` | 声明 Bazel 版本要求（45-48 行） |
| `.bazelversion` | 记录推荐版本（内容：6.5.0） |
| `python/setup.py` | 调用 Bazel 编译 C++ 代码 |
