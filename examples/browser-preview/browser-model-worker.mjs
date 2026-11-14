import { pipeline } from 'https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.8.1';
let generator, engine;
self.onmessage = async ({data}) => {
  try {
    if (data.model === '1.7b') {
      if (!engine) {
        const { CreateMLCEngine } = await import('https://esm.run/@mlc-ai/web-llm@0.2.88');
        engine = await CreateMLCEngine('Qwen3-1.7B-q4f16_1-MLC', {initProgressCallback: p => self.postMessage({type:'progress', progress: p.progress * 100})});
      }
      const tools = data.tools.map(tool => ({type:'function', function:tool}));
      const instruction = `You can use tools. Available tools: ${JSON.stringify(tools)}. To call a tool, output exactly <tool_call>{"name":"tool_name","arguments":{...}}</tool_call>. When you receive a tool result, answer the user. /no_think`;
      const messages = [{role:'system', content: instruction}];
      for (const message of data.messages) {
        if (message.role === 'system') continue;
        if (message.role === 'tool') messages.push({role:'user',content:`<tool_response>${message.content}</tool_response>`});
        else if (message.role === 'assistant' && message.calls?.length) messages.push({role:'assistant',content:message.calls.map(call => `<tool_call>${JSON.stringify({name:call.name,arguments:JSON.parse(call.arguments)})}</tool_call>`).join('')});
        else messages.push({role:message.role,content:message.content});
      }
      const response = await engine.chat.completions.create({messages, temperature:0, max_tokens:512, extra_body:{enable_thinking:false}});
      return output(response.choices[0].message.content || '');
    }
    if (!generator) {
      if (!navigator.gpu) throw new Error('このブラウザでは WebGPU を使えません。WebGPU 対応の Chrome で試してください。');
      generator = await pipeline('text-generation', data.model === '1.7b' ? 'onnx-community/Qwen3-1.7B-ONNX' : 'onnx-community/Qwen3-0.6B-ONNX', {
        device: 'webgpu', dtype: 'q4f16', progress_callback: progress => {
          if (progress.status === 'progress') self.postMessage({type:'progress', progress: progress.progress});
          else if (progress.status === 'ready') self.postMessage({type:'ready'});
        },
      });
    }
    const messages = data.messages.map(message => {
      if (message.role === 'assistant' && message.calls?.length) return {role:'assistant', content: message.calls.map(call => `<tool_call>\n${JSON.stringify({name:call.name, arguments:JSON.parse(call.arguments)})}\n</tool_call>`).join('\n')};
      return {role:message.role, content:message.content};
    });
    const tools = data.tools.map(tool => ({type:'function', function:tool}));
    const prompt = generator.tokenizer.apply_chat_template(messages, {tokenize:false, add_generation_prompt:true, tools, enable_thinking:false});
    const result = await generator(prompt, {max_new_tokens:512, do_sample:false, return_full_text:false});
    const answer = result[0].generated_text;
    output(answer);
  } catch (error) { self.postMessage({type:'error', message:String(error?.message || error)}); }
};

function output(answer) {
  const calls = [];
  const text = answer.replace(/<think>[\s\S]*?<\/think>/g,'').replace(/<tool_call>\s*([\s\S]*?)\s*<\/tool_call>/g, (_, json) => {
    const call = JSON.parse(json);
    calls.push({id: crypto.randomUUID(), name:call.name, arguments:JSON.stringify(call.arguments)});
    return '';
  }).trim();
  self.postMessage({type:'result', text, calls});
}
