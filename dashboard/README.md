# hashring-rs Dashboard

## 启动

使用 Node.js 24，在仓库根目录执行。

```sh
cd dashboard
npm ci
npm run build
npm start
```

打开 [Dashboard](http://localhost:4173/)。

## 实验结果

默认从仓库根目录的 `results/` 读取运行记录。「切换运行」可选择本地结果或内置样例。设置 `RESULTS_DIR` 可指定结果目录，设置 `PORT` 可更改端口。

在 Linux 或 WSL 中启动新实验，需要可执行的 `target/release/hashring-rs`。设置 `HASHRING_BIN` 可指定其他位置。
