import React, {useEffect, useRef, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {Activity, ArrowUpRight, Bot, Braces, Cpu, KeyRound, LoaderCircle, PanelLeftClose, RotateCcw, Settings2, Sparkles} from 'lucide-react';
import {Conversation, ConversationContent, ConversationEmptyState, ConversationScrollButton} from '@/components/ai-elements/conversation';
import {Message, MessageContent, MessageResponse} from '@/components/ai-elements/message';
import {PromptInput, PromptInputTextarea, PromptInputSubmit, PromptInputFooter} from '@/components/ai-elements/prompt-input';
import {TooltipProvider} from '@/components/ui/tooltip';
import {createGatewayFetch} from '../tachyon-gateway.mjs';
import './index.css';

type ChatMessage = {role:string;content:string;call_id?:string;calls?:{id:string;name:string;arguments:string}[]};
type Session = {messages():string;prompt(text:string):Promise<void>;cancel():void;close():Promise<void>;free():void};
type Provider = 'openai'|'browser'|'ollama'|'tachyon';
const labels:Record<Provider,string> = {openai:'OpenAI',browser:'ブラウザ内 / Qwen3',ollama:'ローカル / Ollama',tachyon:'Tachyon Gateway'};
function App() {
 const [provider,setProvider] = useState<Provider>('openai'), [apiKey,setApiKey] = useState(''), [token,setToken] = useState(''), [model,setModel] = useState('gpt-6.1-terra'), [localModel,setLocalModel] = useState('qwen2.5:7b'), [browserModel,setBrowserModel] = useState('1.7b');
 const [messages,setMessages] = useState<ChatMessage[]>([]), [draft,setDraft] = useState(''), [status,setStatus] = useState('WASM を読み込み中'), [busy,setBusy] = useState(false), [ready,setReady] = useState(false), [settings,setSettings] = useState(true), [stream,setStream] = useState('');
 const session = useRef<Session|null>(null), sdk = useRef<any>(null), config = useRef({provider,apiKey,token,model,localModel,browserModel}), gatewayError = useRef('');
 config.current = {provider,apiKey,token,model,localModel,browserModel};
 const refresh = () => { if (session.current) setMessages(JSON.parse(session.current.messages())); };
 useEffect(() => {
  let disposed=false;
  const original=window.fetch.bind(window);
  window.fetch=createGatewayFetch({origin:location.origin,fetchImpl:original,getProvider:()=>config.current.provider,getToken:()=>config.current.token,getOpenAIKey:()=>config.current.apiKey,getOpenAIModel:()=>config.current.model,getLocalModel:()=>config.current.localModel,getBrowserModel:()=>config.current.browserModel,onError:(s:string)=>{gatewayError.current=s},onProgress:(n:number)=>setStatus(`モデルを読み込み中 ${Math.round(n)}%`),onDelta:(s:string)=>setStream(prev=>prev+s)});
  const clear=()=>{setApiKey('');setToken('')}; window.addEventListener('pagehide',clear);
  (async()=>{try {const url='/pkg/pi_rs_browser_sdk.js'; const module=await import(/* @vite-ignore */ url); await module.default(); if(!disposed){sdk.current=module; await open('openai');}} catch {setStatus('WASM を読み込めませんでした。サーバーとビルドを確認してください。')}})();
  return ()=>{disposed=true;window.removeEventListener('pagehide',clear);window.fetch=original};
 },[]);
 async function open(next:Provider) {
  setReady(false); setStatus('セッションを開いています');
  try {if(session.current){await session.current.close();session.current.free();session.current=null;}
   const name=next==='browser'?'browser-qwen-v2':next==='openai'?'openai-preview-v1':next==='ollama'?'ollama-preview-v1':'tachyon-luna-v1';
   session.current=await (next==='browser'||next==='openai'?sdk.current.BrowserSession.open_demo:sdk.current.BrowserSession.open)(name,location.origin+'/demo-gateway',next,'',600000);
   refresh();setStatus('準備完了');setReady(true);
  } catch {setStatus('セッションを開けません。別のタブで同じ会話が開いていないか確認してください。');}
 }
 async function selectProvider(next:Provider){setProvider(next);config.current.provider=next;await open(next)}
 async function run(text:string){
  text=text.trim();if(!text||busy||!session.current)return;
  if(provider==='openai'&&!apiKey.trim()){setSettings(true);setStatus('OpenAI API キーを入力してください');return;}
  if(provider==='openai'&&!model.trim()){setSettings(true);setStatus('モデル名を入力してください');return;}
  if(provider==='tachyon'&&!token.trim()){setSettings(true);setStatus('Tachyon トークンを入力してください');return;}
  if(provider==='ollama'&&!localModel.trim()){setSettings(true);setStatus('モデル名を入力してください');return;}
  gatewayError.current='';setDraft('');setBusy(true);setStream('');setMessages(prev=>[...prev,{role:'user',content:text}]);setStatus('エージェントが考えています');
  try {await session.current.prompt(text);refresh();setStatus('完了');}catch {refresh();setStatus(gatewayError.current||'中断またはエラーが発生しました');}finally{setBusy(false);setStream('')}
 }
 return <div className="app-shell"><aside className="sidebar"><div className="brand"><div className="brand-mark">π</div><div><strong>pi-rs</strong><small>BROWSER AGENT</small></div></div><div className="side-label">WORKSPACE</div><div className="active-nav"><Sparkles size={16}/> エージェント <span>01</span></div><div className="side-info"><div className="side-info-icon"><Cpu size={16}/></div><strong>ブラウザで実行中</strong><p>Rust / WASM の AgentSession がこのタブで動作します。</p><span><i/> WASM HARNESS</span></div><div className="side-bottom"><span>EXPERIMENTAL PREVIEW</span><span>v0.1</span></div></aside>
 <main className="main-panel"><header className="topbar"><div className="crumb"><span>Workspace</span><span className="slash">/</span><b>エージェント</b></div><div className="top-actions"><span className="online"><i/> {ready?'接続準備完了':'準備中'}</span><button className="icon-button" onClick={()=>setSettings(!settings)} aria-label="接続設定"><Settings2 size={18}/></button></div></header>
 <div className="workspace"><section className="chat"><div className="chat-heading"><div><div className="eyebrow"><span/> AGENT SESSION</div><h1>何をお手伝いしましょう？</h1><p>ブラウザの中で動く、あなたの AI エージェント。</p></div><button className="ghost-button" onClick={()=>open(provider)} disabled={busy||!sdk.current}><RotateCcw size={15}/> 会話を再読み込み</button></div>
 <div className="chat-surface"><div className="conversation-label"><span>CONVERSATION</span><span><Activity size={13}/> {labels[provider]}</span></div><Conversation className="conversation"><ConversationContent className="conversation-content">{messages.length===0?<ConversationEmptyState className="empty" icon={<div className="empty-icon"><Sparkles size={22}/></div>} title="会話を始めましょう" description="質問を入力するか、下の例から試してください。"/>:messages.map((m,i)=><React.Fragment key={i}>{(m.role==='user'||m.role==='assistant')&&m.content&&<Message from={m.role} className="chat-message"><div className={'avatar '+m.role}>{m.role==='user'?'U':'π'}</div><div className="message-wrap"><div className="message-name">{m.role==='user'?'あなた':'pi-rs'} {m.role==='assistant'&&<span>AGENT</span>}</div><MessageContent className="message-content"><MessageResponse>{m.content}</MessageResponse></MessageContent></div></Message>}{m.calls?.map(c=><details className="tool-card" key={c.id}><summary><Braces size={15}/> {c.name}<span>ツール呼び出し</span></summary><pre>{c.arguments}</pre></details>)}{m.role==='tool'&&<div className="tool-result"><span>↳ ツール結果</span> {m.content}</div>}</React.Fragment>)}{busy&&<div className="thinking"><LoaderCircle className="spin" size={15}/>{stream||status}</div>}</ConversationContent><ConversationScrollButton/></Conversation>
 {messages.length===0&&<div className="suggestions"><button onClick={()=>setDraft('add_numbers ツールを使って 24 と 18 を足してください。')}> <Braces size={16}/> ツールで計算する <ArrowUpRight size={14}/></button><button onClick={()=>setDraft('Rust の所有権について簡潔に教えてください。')}><Sparkles size={16}/> Rust について聞く <ArrowUpRight size={14}/></button></div>}
 <div className="composer"><PromptInput onSubmit={({text})=>run(text)}><PromptInputTextarea value={draft} onChange={e=>setDraft(e.currentTarget.value)} placeholder="メッセージを入力..." aria-label="メッセージ" disabled={!ready||busy}/><PromptInputFooter><span className="composer-note">Enter で送信 · Shift + Enter で改行</span><PromptInputSubmit status={busy?'streaming':'ready'} onStop={()=>session.current?.cancel()} disabled={!ready||(!busy&&!draft.trim())} aria-label={busy?'キャンセル':'送信'}/></PromptInputFooter></PromptInput><div className="footer-note"><span><i/> {status}</span><span>会話はこのブラウザに保存されます</span></div></div></div></section>
 {settings&&<aside className="settings"><div className="settings-head"><div><div className="eyebrow">CONFIGURATION</div><h2>接続設定</h2></div><button className="icon-button" onClick={()=>setSettings(false)} aria-label="設定を閉じる"><PanelLeftClose size={17}/></button></div><p className="settings-desc">モデルの接続先と認証情報を設定します。</p><label>プロバイダー<select value={provider} disabled={busy||!sdk.current} onChange={e=>selectProvider(e.target.value as Provider)}>{Object.entries(labels).map(([k,v])=><option key={k} value={k}>{v}</option>)}</select></label>{provider==='openai'&&<><label>API キー<div className="input-wrap"><KeyRound size={15}/><input type="password" autoComplete="off" spellCheck={false} placeholder="sk-..." value={apiKey} onChange={e=>setApiKey(e.target.value)} disabled={busy}/></div></label><label>モデル<input value={model} onChange={e=>setModel(e.target.value)} disabled={busy}/></label><p className="field-hint">キーはページのメモリ内でのみ使用し、OpenAI API に直接送信します。API 利用料が発生します。</p></>}{provider==='tachyon'&&<label>アクセストークン<input type="password" autoComplete="off" value={token} onChange={e=>setToken(e.target.value)} disabled={busy}/></label>}{provider==='ollama'&&<label>モデル<input value={localModel} onChange={e=>setLocalModel(e.target.value)} disabled={busy}/></label>}{provider==='browser'&&<><label>モデル<select value={browserModel} onChange={e=>setBrowserModel(e.target.value)} disabled={busy}><option value="1.7b">Qwen3 1.7B · WebLLM</option><option value="0.6b">Qwen3 0.6B · Transformers.js</option></select></label><p className="field-hint">初回はモデルのダウンロードと WebGPU が必要です。</p></>}<div className="setting-divider"/><div className="capability"><div className="capability-icon"><Bot size={17}/></div><div><strong>エージェントの機能</strong><p>このデモでは add_numbers ツールを WASM 内で実行します。</p></div></div><div className="setting-divider"/><div className="runtime"><span>RUNTIME</span><b>Rust + WebAssembly</b><span>SESSION STORAGE</span><b>このブラウザ</b></div></aside>}</div></main></div>
}

createRoot(document.getElementById('root')!).render(<TooltipProvider><App/></TooltipProvider>);
