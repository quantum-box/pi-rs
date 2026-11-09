# 受入記録

この記録は P-PLT-125 の追加 issue を対象にする。AIP・Field・Workspace の本番提供可否は、ローカルの試験結果と別に扱う。既存 AIP のコードを使った結果は [検証用 AIP 構成の PR](https://github.com/quantum-box/tachyon-apps/pull/13288) に記録した。

| 対象 | 証拠 | 残る検証 |
| --- | --- | --- |
| PLT-6367、6368 | [対応表と依存方向](architecture.md)。コア、Ollama、CLI、保存ポートを分離。 | |
| PLT-6369、6388 | [7課題・3反復の比較](expanded-evaluation.md)。ツールの効果と回答の独立判定。[AIP Evaluation の判定](../evaluation/aip-quality-gate-results.json)も記録。 | |
| PLT-6370〜6377 | [実行・保存契約](extensions.md)。SSE 切断、tool_call 対応、追加入力の重複、v1 から v2 への追記、圧縮、Skill の版、CLI とライブラリの共通経路を検証。 | |
| PLT-6378、6379 | 依存循環、無効な依存と拡張、未知の状態保持。候補バイナリの検証と復帰を実行。 [拡張サイクル](../evaluation/extension-cycle-results.json) | |
| PLT-6380、6382、6383 | [AIP ホスト側の受入テスト](../crates/aip-host/tests/acceptance.rs)。権限取消、期限切れ承認、対象版変更、再開と操作照会、予算枯渇を検証。AIP の予算ユースケースと既存ポートの結合テストで、別 Run の予算の拒否、ジャーナルの再オープン後の重複精算の防止、MCP の権限取消、Coding Job 応答消失後に、既存の永続実行状態からジョブを照会。 | 運用の Run 所有者から承認を供給し、実環境の認証・接続で受入。 |
| PLT-6381 | [版・鮮度・テナントを検査する参照拡張](../crates/aip-host/src/memory.rs)。モデルへの注入は許可と鮮度を照会した後。既存の Work State を隔離 TiDB で保存し、別テナント・プロジェクト、版更新、期限切れと認可取消を確認。[有効・無効のモデル計測](expanded-evaluation.md#参照メモリの有効無効比較)を記録。 | |
| PLT-6384 | [計画モード](../crates/plan-mode/src/lib.rs)。許可された読取りツールに制限し、計画状態を branch ごとに復元。 | |
| PLT-6385 | [分岐レビュー拡張](../crates/review/src/lib.rs)。親の選択した記録を子へ渡し、読取り権限を絞って結果を戻す。AIP の親子予算を隔離 TiDB で検証。子のツールは親の対象・版と照合する。 | 永続の Run 所有者を接続した停止と再開。 |
| PLT-6386 | [候補の生成・検証・再開・旧版への復帰](../evaluation/extension-cycle.mjs)。模擬操作は1件のまま。 | |
| PLT-6387 | SSE、操作応答消失、journal の末尾、権限取消、古い承認、再起動、圧縮の失敗をローカルの故障テストで再現。 | 既存サービスへの結合時に、外部効果、予算、監査 ID が一致することを確認。 |
| PLT-6389 | [採用判断と段階的な統合条件](adr/001-aip-evaluation-boundary.md)。実装済みの検証用経路を採用し、結合受入をテナント経路の有効化条件とした。 | |

再現は `cargo test --locked --workspace --all-features` と
`cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`。
拡張の候補は `node evaluation/extension-cycle.mjs` で別バイナリへ組み込み、検証用セッションで確認してから開き直す。


## 故障ケースの結合受入（PLT-6387）

| 故障 | 期待する状態 | 確認できた結果 |
| --- | --- | --- |
| モデル応答の途中切断 | 部分回答を完了扱いせず、使用量を照合待ちにする。 | Ollama SSE と AIP ストリームの切断を再現。AIP の予算台帳では照合待ちが1件で、確認した使用量を再送しても精算は1回。 |
| 外部操作後の応答消失 | 再送せず操作 ID で照会する。取消要求は停止確認と区別する。 | Coding Job の模擬受付は1回。隔離 TiDB の既存実行状態から、応答消失後の受付中と完了後の両方で同じジョブを照会した。停止を確認するまで完了扱いにしない。既存 Workspace の受付で、別ユーザーを拒否し、再送で同じジョブ ID を返し、取消後の別ターンを拒否した。Field 互換の隔離 HTTP サーバーで書込み後に503を返し、同じ Run のジャーナルを開き直して読取りだけで下書き ID を回収した。POST と予算精算は1回ずつ。 |
| Workflow Run の実行中の取消 | 同じ Run の Goal ターンへ取消を伝え、遅れた結果で Run を進めない。中断時の使用量は照合待ちとする。 | Workflow ワーカーの試験で取消通知と遅延結果の破棄を確認。Harness の試験ではジャーナルに取消と未確定使用量が残り、予算台帳のメモリ実装では照合待ち1件、精算0件。 |
| Harness 完了後の Workflow Goal 保存失敗 | 同じターンから確定済みの結果を読み、追加のモデル呼出しなしで Goal に反映する。 | 既存の Workflow Goal 所有者から2ターンを実行。2ターン目の Goal 保存を失敗させ、再送で同じ Run／Goal／セッションの完了結果と合計48トークンを回収。モデルへの送信は合計2件。Goal 保存には試験用リポジトリを使用。 |
| 保存途中での停止 | 不完全な末尾を除き、確定済みのセッションを復元する。 | ファイル末尾を破損させ、確定済みの記録と操作 ID を保持した。 |
| 子レビューの中断 | 親子の関係を残し、同じ子 Run から再開する。 | キャンセルを伝播。AIP の予算台帳で中断した子を照合し、ジャーナルを開き直して同じ子 Run の結果を親へ返した。 |
| 圧縮の失敗 | 既存の文脈を残し、同じ入力から再開できる。 | 要約を途中で中断しても既存の分岐と保留中の入力を保持した。 |
| 重複配送・予算枯渇 | 入力と精算を重複させず、予算外の呼出しを拒否する。 | 追加入力の再送を除外。TiDB で確定済み Run の再送後も精算1件。予算枯渇のハーネス試験では外部操作0件。 |
| 権限取消・古い承認・対象の版変更 | 実行前に再評価し、許可を流用しない。 | 既存 AIP Runtime の HTTP MCP クライアントで、検出後の権限取消と接続設定変更を拒否。許可を戻した接続では構造化結果を受領。旧承認と対象変更は委譲の試験で拒否。保存済み参照はテナント・プロジェクト・鮮度・版・認可を照合した。 |
| 拡張の非互換な版 | 明示的に拒否し、既存セッションを保持する。 | 候補ビルドで非対応版を拒否。旧版への復帰後も合成の外部操作は1件。 |

AIP の予算と作業状態には既存の SQLx 実装を使い、MCP は既存 Runtime から隔離 HTTP サーバーへ接続した。Coding Job の実行状態と Workspace の受付には既存の SQLx 実装を使い、Gateway、認証、Coding Job の受付・停止の一部には試験用サービスを使った。Field の作成・一覧には既存 Workflow の HTTP クライアントを使い、相手先と承認を隔離した。残る受入は、実行所有者が発行した承認・Run・監査 ID を使って実際の Workspace と業務 API の操作記録を突き合わせること。 [AIP 結合試験](https://github.com/quantum-box/tachyon-apps/pull/13288) · [ハーネス試験](../tests/harness.rs)


## 既存 Workflow Goal からの起動（PLT-6382）

隔離した AIP の評価構成では、既存 Workflow が Goal の実行権を取得してから Rust
Harness を呼ぶ。選択結果は Goal セッションに記録する。AIP API は評価構成の
設定がある場合に起動を拒否する。評価ストリームは Run の予算予約を使うため、
運用への組込みには既存 Payment 原票、価格版、Field 配送を通る Gateway 接続が
必要。共有の非公開永続ジャーナルと、認可済みツールプロファイルも必要。
[実装と検証記録](https://github.com/quantum-box/tachyon-apps/commit/5b46814a3c64403d7f1bcccc2be3da39e6adb4b2)


取消は Workflow ワーカーのリース更新時に検出する。既定の確認間隔は200秒。
一時停止では現在のターンが終わってから次のターンを止める。Workflow ワーカーの
試験22件と AIP 統合試験10件を確認した。
[取消の実装と検証記録](https://github.com/quantum-box/tachyon-apps/commit/a2148425702a40a5719e231a2bc8d0cbfbc9d4c3)


Field の隔離結合試験では、Workflow Run、Harness Run、セッション、モデル要求、
外部操作 UUID が監査ログで一致した。書込み応答の消失後に、同じ操作 UUID で
再照会と確定を追跡した。入力、応答、認証トークン、外部の参照値はログに出なかった。
AIP 統合試験10件と課金入口の検査58件が通過した。
[監査と課金境界の実装](https://github.com/quantum-box/tachyon-apps/commit/030c4d569ff7c3a11ddf763b7f437a10a51adaf8)

隔離評価の Workflow ストリームを既存の Gateway Payment 処理につないだ。
Payment が拒否した場合はモデル送信0件。正常時にはキャッシュ使用量、価格版、
Workflow Run の親 ID を既存の `llms_chat_completion` 原票の形式で Payment に
渡す。Goal 保存失敗後の再送でもモデル要求と Payment 呼出しは各2件のままで、
Payment キーはジャーナルのモデル要求 UUID と一致した。Gateway の課金試験
14件と AIP 統合試験10件が通過した。Payment と料金表には試験用実装を使った。
実際の請求記録、課金結果が不明な場合の復旧、Field への配送は受入が残っている。
[Payment 接続と検証記録](https://github.com/quantum-box/tachyon-apps/commit/afd3d0bcda2fd1a9487135fbab3e769345aebc07)

モデルが報告した使用量とキャッシュ内訳は、Payment の書込みより先にジャーナルへ
確定する。AIP の試験では初回の Payment 書込みを失敗させ、セッションを開き直して
同じモデル要求 UUID と使用量を再送した。モデル呼出しは1回、Run 予算の精算は
1件だった。再照合以降の実行を止めるため、試験は取消済みのトークンを渡している。
AIP 統合試験11件が通過した。元の回答の復元、実際の Payment 原票と Field への
配送は受入が残っている。
[Payment 書込み失敗後の復旧試験](https://github.com/quantum-box/tachyon-apps/commit/0a43bb9d01750fd04482bc319f231978884fb559)
