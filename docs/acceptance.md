# 受入記録

この記録は P-PLT-125 の追加 issue を対象にする。AIP・Field・Workspace の本番提供可否は、ローカルの試験結果と別に扱う。既存 AIP のコードを使った結果は [検証用 AIP 構成の PR](https://github.com/quantum-box/tachyon-apps/pull/13288) に記録した。

| 対象 | 証拠 | 残る検証 |
| --- | --- | --- |
| PLT-6367、6368 | [対応表と依存方向](architecture.md)。コア、Ollama、CLI、保存ポートを分離。 | |
| PLT-6369、6388 | [7課題・3反復の比較](expanded-evaluation.md)。ツールの効果と回答の独立判定。[AIP Evaluation の判定](../evaluation/aip-quality-gate-results.json)も記録。 | |
| PLT-6370〜6377 | [実行・保存契約](extensions.md)。SSE 切断、tool_call 対応、追加入力の重複、v1 から v2 への追記、圧縮、Skill の版、CLI とライブラリの共通経路を検証。 | |
| PLT-6378、6379 | 依存循環、無効な依存と拡張、未知の状態保持。候補バイナリの検証と復帰を実行。 [拡張サイクル](../evaluation/extension-cycle-results.json) | |
| PLT-6380、6382、6383 | [AIP ホスト側の受入テスト](../crates/aip-host/tests/acceptance.rs)。権限取消、期限切れ承認、対象版変更、再開と操作照会、予算枯渇を検証。AIP の予算ユースケースと既存ポートの結合テストで、別 Run の予算の拒否、ジャーナルの再オープン後の重複精算の防止、MCP の権限取消、Coding Job 応答消失後の照会を確認。 | 運用の Run 所有者と承認・操作照会を接続し、実ストアと業務 API で検証。 |
| PLT-6381 | [版・鮮度・テナントを検査する参照拡張](../crates/aip-host/src/memory.rs)。モデルへの注入は許可と鮮度を照会した後。既存の Work State を隔離 TiDB で保存し、別テナント・プロジェクト、版更新、期限切れと認可取消を確認。[有効・無効のモデル計測](expanded-evaluation.md#参照メモリの有効無効比較)を記録。 | |
| PLT-6384 | [計画モード](../crates/plan-mode/src/lib.rs)。許可された読取りツールに制限し、計画状態を branch ごとに復元。 | |
| PLT-6385 | [分岐レビュー拡張](../crates/review/src/lib.rs)。親の選択した記録を子へ渡し、読取り権限を絞って結果を戻す。AIP の親子予算を隔離 TiDB で検証。子のツールは親の対象・版と照合する。 | 永続の Run 所有者を接続した停止と再開。 |
| PLT-6386 | [候補の生成・検証・再開・旧版への復帰](../evaluation/extension-cycle.mjs)。模擬操作は1件のまま。 | |
| PLT-6387 | SSE、操作応答消失、journal の末尾、権限取消、古い承認、再起動、圧縮の失敗をローカルの故障テストで再現。 | 既存サービスへの結合時に、外部効果、予算、監査 ID が一致することを確認。 |
| PLT-6389 | [採用判断と段階的な統合条件](adr/001-aip-evaluation-boundary.md)。実装済みの検証用経路を採用し、結合受入をテナント経路の有効化条件とした。 | |

再現は `cargo test --locked --workspace --all-features` と
`cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`。
拡張の候補は `node evaluation/extension-cycle.mjs` で別バイナリへ組み込み、検証用セッションで確認してから開き直す。
