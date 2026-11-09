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
| PLT-6385 | [分岐レビュー拡張](../crates/review/src/lib.rs)。親の選択した記録を子へ渡し、読取り権限を絞って結果を戻す。AIP の親子予算を隔離 TiDB で検証。子のツールは親の対象・版と照合する。取消後に親子のジャーナルを開き直し、元の子 Run だけを再開できることを検証した。 | 運用接続での受入。 |
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
Harness を呼ぶ。選択結果は Goal セッションに記録する。AIP API はビルド機能と
運用設定の両方を指定した評価 Run だけを対象にし、既存の Gateway Payment と
料金方針を使う。共有の非公開永続ジャーナルが必要。外部ツールは別途、ホストの
認可済みプロファイルに登録する。
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
同じモデル要求 UUID と使用量を再送した。元の回答を復元し、モデル呼出しは1回、
Run 予算の精算は1件だった。通常の Workflow Goal 経路では、同じ実行権の内側で
一度だけ再照合し、2ターンを完了した。ハーネスの試験では保存済みの操作 ID が
一致し、精算前に0件、再開後に1件の外部実行を確認した。AIP 統合試験11件が
通過した。この復旧試験のモデルと Payment には試験用実装を使用した。
[Payment 書込み失敗後の復旧試験](https://github.com/quantum-box/tachyon-apps/commit/5ab961ff5d3c493845014643960043c37135dbdf)


## Payment 原票と Field 互換 HTTP への配送（PLT-6382、PLT-6387）

隔離 MySQL の既存 Payment 台帳で、明細の保存後に原票の書込みを失敗させた。
同じ請求キーを並行再送し、確定後にも再送して、明細と原票が各1件であることを
確認した。既存の ERP 収集と配送は同じ請求キーに紐付く入出力・キャッシュ作成・
キャッシュ読取りの4レコードを Field 互換 HTTP サーバーへ各1回送った。
回帰試験を含め MySQL の対象3件が通過した。料金表と受信先は試験用で、運用テナント
の認可と実際の Field 側の受入検証は残っている。
[SQL 原票と配送の結合試験](https://github.com/quantum-box/tachyon-apps/commit/12a554b4b47a3c02d5d525918e5495705bec7985)


## AIP API での対象 Run の指定

`pi-rs-evaluation` のビルド機能と明示的な評価設定を使い、テナント、Run、ステップ、
実行ユーザー、モデルを照合する。モデル送信前には既存の AI プラン、Gateway の
モデル認可、Run の予算予約と Payment の事前審査を通す。API の試験211件が通過し、
評価機能を省いたビルドの確認も通過した。外部ツールは設定なしでは空。
運用環境への適用と実際の Field での受入検証は残っている。
[AIP API の接続と試験](https://github.com/quantum-box/tachyon-apps/commit/15123aa4c510a2274862859d44ad680d4705ab8e)


## Linear の読取り専用 MCP（PLT-6380、PLT-6383）

対象 Run の `linear_read` 設定でテナントの既存接続 ID と、`get_issue` または
`list_issues` を指定できる。既存の Runtime が接続を照会し、Linear の読取り専用
MCP URL を使う。試験では書込み設定と既存接続の差し替えを拒否し、隔離 HTTP
サーバーで許可取消後の追加送信が0件であることを確認した。AIP 統合試験11件、
Runtime の接続試験4件、AIP API の試験212件が通過した。

読取り前に既存の Workflow Run 予算から委譲1回分を予約する。隔離 TiDB と HTTP
MCP の組合せで、許可取消後、同じ操作 ID の再送時、予算枯渇後のいずれも送信が
増えず、精算は元の操作 ID の1件のみであることを確認した。応答が確認できない場合は
照会待ちにし、精算済みの操作は照会時も再送しない。AIP 統合試験12件と API 設定
試験4件が通過した。AIP 経由の実接続と添付本文の取扱いは受入が残っている。

[予算と試験](https://github.com/quantum-box/tachyon-apps/commit/4c55be4bf7134c1a26cff3735fbbecdc3155253f)

応答待ちを中断したときは、元の接続で元の要求 ID に対する MCP 取消を通知する。
隔離 HTTP MCP で、応答を返さない操作の取消通知を受信したことを確認した。隔離
TiDB では、同じ操作 ID の照会で使用量確認待ちの記録が1件、同じ操作 ID への再送が
0件、確定済みの別操作の精算が1件だった。サーバー側の停止確認は運用接続での受入に残る。
[取消通知と試験](https://github.com/quantum-box/tachyon-apps/commit/4e55b9b3acd2666d9574c1863f8f69c25590ec0b)

[接続と試験](https://github.com/quantum-box/tachyon-apps/commit/226dd8830ebf47a6a17174b5ffd1d5f9761f2b8f) · [Linear MCP](https://linear.app/docs/mcp)

### Linear 添付参照（PLT-6383）

`get_issue` の添付は取得先 URL と外部添付 ID を除いた参照で返す。
`lookup_attachment` は既存の読取り MCP に同じ課題を照会し、現在の添付との対応を
確認する。隔離 HTTP MCP で署名更新後も参照でき、別の課題・Workspace、取得先変更、
添付削除では照会を拒否した。接続許可の取消後は追加送信0件。既存の Linear
コネクターでは添付の形を確認した。統合試験13件、AIP 設定試験5件が通過。
[添付参照と試験](https://github.com/quantum-box/tachyon-apps/commit/018ceaa0438d5f13ec7a1ae9dad57c5241e674ec)

## 子レビューの再開と親予算（PLT-6385、PLT-6387）

取消後に親子のジャーナルを開き直し、別の子 Run ID を指定した再開を拒否した。
元の Run で完了した後、同じ再開要求は保存済みの回答を返し、モデル使用量と
ジャーナルは変化しなかった。同じファイルを開いている間は2人目の実行者も拒否した。

隔離 TiDB では同じ Run 名を持つ別の親予算を渡しても、子のモデル呼出しと
ジャーナルへの書込みが0件であることを確認した。正しい親予算では応答切断後に
両方のジャーナルを開き直して続行し、切断時の未確認使用量を同じ要求 ID で照合した。
評価設定で対象ターンを指定すると、既存 Workflow Goal の実行権で子レビューを開始する。
停止信号で子を中断し、子予算に使用量確認待ちを1件記録した。親モデルの送信は0件。
子の課金書込みを失敗させた試験では同じ要求 ID で精算を再試行し、親子の完了報告を
再取得してもモデル呼出しは3件、Goal の使用量は72トークンのままだった。課金は親
Workflow に紐づけ、子は既存の子予算で精算する。統合試験12件、設定試験5件が通過。
モデル、Payment、Goal 保管先は試験用。
[Workflow 組込みと試験](https://github.com/quantum-box/tachyon-apps/commit/43f8eaeb68483a0729e4a3bc21fb62aa23c35725)

[ホストの再開試験](https://github.com/quantum-box/pi-rs/commit/7c371f11a51665c4b9e0edd8fabd6c93e69dedd4) · [AIPの予算試験](https://github.com/quantum-box/tachyon-apps/commit/707ca5784b8ed499722aba6a5d920be0421de7c7)


隔離 TiDB の Goal 行で失効した実行権を2つのワーカーが同時に取り直すと、同じ
ターン番号の更新が1件だけ成立した。子の課金書込みを2回失敗させた Workflow
試験では、保存済み応答を同じターンで引き継ぎ、同じ要求 ID で精算した。親子
合わせてモデル送信は3件、Goal の使用量は72トークンで、完了報告の再取得でも
増えなかった。応答と使用量が確認できない子レビューの引継ぎは拒否した。
運用環境での停止・再開の受入が残っている。
[実行権の引継ぎと試験](https://github.com/quantum-box/tachyon-apps/commit/dca333c4e8cc3627b686be59863709159384ae1d)

## Coding Job の委譲予算（PLT-6380、PLT-6382、PLT-6387）

既存 Run 予算から委譲1回分を予約し、Workspace と操作の承認を受付直前に再確認する。
隔離 TiDB の予算木と実行状態を使い、予算枯渇時の受付は0件だった。受付後に応答を
失うと、元の操作を使用量確認待ちとして記録した。別 Workspace の照会では精算せず、
正しい Workspace の受付を照会した時点で同じ操作 ID に1件だけ精算した。停止の
確認まで再送や完了扱いは発生しなかった。受付、承認と停止確認は試験用で、
運用 Workspace の受付先と承認元の接続は残っている。
[Coding Job の予算と試験](https://github.com/quantum-box/tachyon-apps/commit/ab06f831b047b9f44df8e48a8a147048f81b9221)

### Workspace 受付における承認内容の照合

承認したハーネス操作 ID、対象版、期限、対象と入力のハッシュを既存の専用実行状態に
記録する。ハーネスで別の操作 ID・対象版・入力を拒否し、隔離 TiDB の Workspace
受付記録では別 actor、期限切れ、Workspace・リポジトリ・ブランチ・入力の変更を
拒否した。拒否後に残るジョブ行は0件。元の条件で再送すると同じジョブを返し、
停止後は再受付を拒否した。運用の承認元と、Workspace がコマンドを起動する直前の
照合は接続作業が残っている。
[承認の照合と試験](https://github.com/quantum-box/tachyon-apps/commit/9dd9ee2c3ca8110afbe9ef11381f1eabd894fa20)
