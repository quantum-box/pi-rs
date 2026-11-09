# 受入記録

この記録は P-PLT-125 の追加 issue を対象にする。AIP・Field・Workspace の本番提供可否は、ローカルの試験結果と別に扱う。

| 対象 | 証拠 | 残る検証 |
| --- | --- | --- |
| PLT-6367、6368 | [対応表と依存方向](architecture.md)。コア、Ollama、CLI、保存ポートを分離。 | |
| PLT-6369、6388 | [7課題・3反復の比較](expanded-evaluation.md)。ツールの効果と回答の独立判定。[AIP Evaluation の判定](../evaluation/aip-quality-gate-results.json)も記録。 | |
| PLT-6370〜6377 | [実行・保存契約](extensions.md)。SSE 切断、tool_call 対応、追加入力の重複、v1 から v2 への追記、圧縮、Skill の版、CLI とライブラリの共通経路を検証。 | |
| PLT-6378、6379 | 依存循環、無効な依存と拡張、未知の状態保持。候補バイナリの検証と復帰を実行。 [拡張サイクル](../evaluation/extension-cycle-results.json) | |
| PLT-6380、6382、6383 | [AIP ホスト側の受入テスト](../crates/aip-host/tests/acceptance.rs)。権限取消、期限切れ承認、対象版変更、再開と操作照会、予算枯渇、精算の重複を模擬境界で検証。 | 実際の AIP Run と Coding Job／Workspace、MCP、業務 API を結んだ受入。 |
| PLT-6381 | [版・鮮度・テナントを検査する参照拡張](../crates/aip-host/src/memory.rs)。モデルへの注入は許可と鮮度を照会した後。 | 既存の AIP 保存先に接続した権限取消と利用量測定。 |
| PLT-6384 | [計画モード](../crates/plan-mode/src/lib.rs)。許可された読取りツールに制限し、計画状態を branch ごとに復元。 | |
| PLT-6385 | [分岐レビュー拡張](../crates/review/src/lib.rs)。親の選択した記録を子へ渡し、読取り権限を絞って結果を戻す。 | AIP の親子 Run と共有予算、停止を同じ境界で検証。 |
| PLT-6386 | [候補の生成・検証・再開・旧版への復帰](../evaluation/extension-cycle.mjs)。模擬操作は1件のまま。 | |
| PLT-6387 | SSE、操作応答消失、journal の末尾、権限取消、古い承認、再起動をローカルの故障テストで再現。 | 既存サービスへの結合時に、外部効果、予算、監査 ID が一致することを確認。 |
| PLT-6389 | [採用判断と段階的な統合条件](adr/001-aip-evaluation-boundary.md)。 | 結合受入の結果を反映。 |

再現は `cargo test --locked --workspace --all-features` と
`cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`。
拡張の候補は `node evaluation/extension-cycle.mjs` で別バイナリへ組み込み、検証用セッションで確認してから開き直す。
