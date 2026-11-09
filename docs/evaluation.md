# 評価と採用範囲

対象は Linear の P-PLT-125。採用範囲を、開発・検証用の独立した Rust ライブラリと CLI に限定する。Tachyon の既存実行経路を置き換える判断は、テナント境界での検証と、実業務での費用・介入率の測定が揃うまで保留する。 [Linear プロジェクト](https://linear.app/quantum-box/project/rust-agent-harness-pi%E3%81%AE%E3%82%B3%E3%82%A2%E6%8B%A1%E5%BC%B5%E3%82%92%E5%86%8D%E5%AE%9F%E8%A3%85-cd933e2f2ff7/overview)

## M1 既存実装との境界

比較対象は Pi v1.5.0、コミット `f9a26365568127461c47a07a37dcb42c140a3ac5`。実行ループ、履歴ツリー、文脈加工、拡張イベントを設計の参考にした。Rust のコアと拡張は独立実装で、Pi は比較時に npm の固定版 SDK で実行する。 [Pi v1.5.0 ソース](https://github.com/earendil-works/pi/tree/f9a26365568127461c47a07a37dcb42c140a3ac5)

Tachyon Apps の `cc3a0da61e434297fc73e904d288b812baf4b0f6` と Field の `bd047e2180b4cd0bd7de01464d1144aabb79d12d` を読み、定義だけでなくルートと実装の所在を照合した。AIP API の ECS 昇格ジョブと、両ランナーのスモーク成功も確認した。これらは既存環境の運用記録であり、pi-rs の接続試験はローカルの検証環境で行った。 [AIP API の昇格結果](https://github.com/quantum-box/tachyon-apps/actions/runs/42461601459) [ランナーの検証結果](https://github.com/quantum-box/tachyon-apps/actions/runs/42450747546)

| 領域 | 現行の提供口と責務 | 採用時の接続方針 |
| --- | --- | --- |
| AIP Agent | AgentGoal が継続や待機を管理し、セッション履歴と実行費用の永続化を持つ。 | Rust のループを使う場合も、AIP がターンを起動し再開を決める。Rust は 1 回の実行と、その文脈・ツール結果を返す。 [AgentGoal と実行ドメイン](https://github.com/quantum-box/tachyon-apps/tree/cc3a0da61e434297fc73e904d288b812baf4b0f6/platform/aip/runtime/agent/domain/src) |
| AIP Workflow / Model Gateway | WorkflowRun が定義スナップショット、ステップ、レビュー、待機と復旧を保持する。モデル利用量と予算は既存の Gateway 側で扱う。 | WorkflowRun と既存の使用量計測を再利用する。Rust の制限はローカル実行の上限として扱い、課金や永続ワークフローの主体は AIP に揃える。 [WorkflowRun 契約](https://github.com/quantum-box/tachyon-apps/blob/cc3a0da61e434297fc73e904d288b812baf4b0f6/platform/aip/contract/src/workflow_runtime.rs) |
| MCP | AIP Agent の `McpInvoker` と `McpHub` がサーバーの検索・ツール呼出し・認証を処理する。 | 実運用では AIP 側の認可済み invoker を `Tool` で包む。Rust 側の検証ではファイル、固定コマンド、読み取り専用 API を使用した。 [MCP 呼出し口](https://github.com/quantum-box/tachyon-apps/blob/cc3a0da61e434297fc73e904d288b812baf4b0f6/platform/aip/runtime/agent/src/tool/mcp.rs) |
| Workspace Runtime / Coding Job | Runtime の内部 API には exec の start・stop・inspect、試行の世代付きコールバック、用途別資格情報の境界がある。AIP のテナント向けコマンドは Coding Job に委譲する方針。 | 本番ではコマンドを既存の隔離実行へ委譲する。Runtime が提供する照会経路に操作 ID と試行世代を対応させる。 [Runtime exec 契約](https://github.com/quantum-box/tachyon-apps/blob/cc3a0da61e434297fc73e904d288b812baf4b0f6/platform/contract/workspace_runtime/src/exec_api.rs) [ADR-0157](https://github.com/quantum-box/tachyon-apps/blob/cc3a0da61e434297fc73e904d288b812baf4b0f6/docs/src/architecture/decisions/ADR-0157-agent-runtime-delegates-commands-to-coding-job.md) |
| Field | 業務 Action と業務承認を持つ。業務操作ごとのキーと対象レコードから、適用済み・未適用・不明を照合する。 | Rust 拡張の事前チェックを補助に使い、実際の承認と操作の認可は Field の実行境界で強制する。Field が対応する本文キーや自然キーを使い、結果が不明なら既存の照合経路へ渡す。 [Field の復旧手順](https://github.com/quantum-box/tachyonfield/blob/bd047e2180b4cd0bd7de01464d1144aabb79d12d/docs/runbooks/field-aip-workflow-side-effects.md) |

## M2・M3 実装と検証

- ライブラリと CLI の共通経路で、モデル要求、SSE、引数検証、ツール結果、追加入力、中断、分岐、圧縮後の続行を通した。実モデルによる要約の後、要約だけを使ってコードを再取得できた。生成要約には完了済みの作業を未完了とする誤りが 1 件あった。テストでは別セッションの並行実行、同一ジャーナルの排他、SSE 終端の欠落、部分レコードの復旧、ポリシー拒否を確認した。
- 外部サービスを模したテストで、ツール結果を返す前に実行を停止した。再起動後は結果を不明とし、未開始の呼出しはスキップした。操作 ID で外部状態を照合し、復元後も外部効果の記録は 1 件だった。試したのはハーネスの境界契約で、Field や Runtime への本番書込みは行っていない。
- 拡張の更新は別の作業コピーで検証した。既存の workbench crate の 1 ファイルだけで v1 のカウンターを v2 に差し替え、コア変更 0 ファイル、再ビルド 1 回、プロセス再起動 1 回だった。初回 release ビルドは 20.87 秒、差し替え後の増分ビルドは 3.12 秒。v1 の `1` は v2 の `tool_results: 1` へ移行し、同じセッションで次のツール実行後に `2` になった。Pi の比較側は SDK のインライン拡張とツール定義を読み込み、追加のコンパイル手順は 0 回だった。どちらも実装者の編集時間は測っていない。
- 資格情報はトランスポート側で渡し、記録するモデル要求には含めない。通常の構造化イベントはツールの内容を省く。ローカルジャーナルは会話の復元用に本文を保存する。本番候補には、AIP の既存マスキングとテナント単位の保管・保持期間を適用する必要がある。 [Coding Job のマスキング](https://github.com/quantum-box/tachyon-apps/blob/cc3a0da61e434297fc73e904d288b812baf4b0f6/platform/aip/runtime/agent/src/coding_job/redaction.rs)

## M4 比較条件

- 同じ Apple M5 Pro のホストで、Ollama v0.40.15、`qwen2.5:7b` Q4_K_M、モデルダイジェスト `845dbda0ea48ed749caafd9e6037047aa19acfcfd82e704d7ca97d631a0b697e` を使用した。モデルを一度ウォームアップし、実行順を交互に入れ替えた。
- 新規セッションで 3 課題を各 3 回実行した。共通のシステムプロンプト、ユーザープロンプト、ツール定義、ファイル、API 応答を使用した。プロキシで最大 6 リクエスト、各 512 出力トークン、温度 0、seed 123 を揃えた。各試行の外部制限は 60 秒。Pi のリトライと自動圧縮は無効にした。
- 実際のリクエストは JSON キー順、生成されたツール呼出し ID、文字列とテキスト配列の表現差を正規化し、ツール呼出しと結果の対応を含めて比較した。各ハーネス固有のプロセス起動、セッション保存、ツール実装は計測に含む。小さな合成課題であり、本番の承認待ちや長時間作業は測っていない。

| 課題 | Rust 成功 | Pi 成功 | 時間の中央値 Rust / Pi | 入力 / 出力トークン（両者共通、1 試行） |
| --- | ---: | ---: | ---: | ---: |
| ファイルのコード読取り | 3/3 | 3/3 | 933 / 1104 ms | 549 / 27 |
| 固定コマンドで行数取得 | 3/3 | 3/3 | 740 / 1106 ms | 547 / 23 |
| 業務 API と行数を組み合わせる | 0/3 | 0/3 | 1636 / 1850 ms | 623 / 58 |

両者とも成功は 6/9。全 9 組で初回・後続の文脈とツール定義が一致した。業務 API 課題では両方が正しいツールを使い、値も得たが、指定の `Sable|Friday|7` に対し `carrier|Sable|eta|Friday|count|7` を返したため失敗とした。モデル API の従量費は両者とも $0、人による介入は 0 回。ローカル電力・機材費は未計測。 [試行ごとの記録](../evaluation/results.json)

今回の結果から、既存 AIP を置き換えるほどの成功率やコストの差は確認できなかった。開発環境での Rust 拡張とセッション復旧の検証に採用し、テナント向けコマンド、WorkflowRun、MCP、Field 承認は既存基盤に任せる。将来の接続候補は、AIP が所有する読み取り専用の 1 ターンに絞る。サブエージェントや長期記憶の実装判断はこの採用範囲に含めない。

## 再実行

Node.js 22.19 以上を用意する。Ollama のコンテキスト長を 4096、並列度を 1 にして起動する。

```sh
ollama pull qwen2.5:7b
cargo build --locked --release --workspace
npm ci --ignore-scripts --prefix evaluation
node evaluation/compare.mjs
```

`OLLAMA_URL` と `PI_RS_BIN` で検証先を指定できる。モデルのダイジェストが一致しない場合は終了する。異なるモデルで別の比較を行う場合は `EVAL_MODEL` と `EVAL_MODEL_DIGEST` を指定する。
