import {
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
} from 'n8n-workflow';

type StageConfig = {
  name: string;
  enabled: boolean;
  prompt: string;
  model?: string;
  temperature?: number;
  maxTokens?: number;
  format?: string;
  condition?: string;
  saveOutput?: boolean;
};

export class OpenAITextRefinery implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'OpenAI Text Refinery (Pro)',
    name: 'openaiTextRefinery',
    icon: 'file:openai.svg',
    group: ['transform'],
    version: 1,
    description: 'Multi-stage OpenAI editing pipeline (chunking, memory, tail, persistence)',
    defaults: { name: 'OpenAI Text Refinery (Pro)' },
    inputs: ['main'],
    outputs: ['main'],
    credentials: [
      {
        name: 'openAiAccountApi',
        required: true,
      },
      {
        name: 'googleDriveOAuth2Api',
        required: false,
      },
    ],
    properties: [
      // General
      {
        displayName: 'Input Field',
        name: 'inputField',
        type: 'string',
        default: 'text',
        description: 'Which incoming JSON field contains the text to process',
      },
      {
        displayName: 'Default Model',
        name: 'defaultModel',
        type: 'options',
        options: [{ name: 'gpt-5', value: 'gpt-5' }, { name: 'gpt-4.1', value: 'gpt-4.1' }],
        default: 'gpt-5',
      },
      {
        displayName: 'Default Temperature',
        name: 'defaultTemperature',
        type: 'number',
        default: 0.05,
      },
      // Chunking
      {
        displayName: 'Chunk Size (chars)',
        name: 'chunkSize',
        type: 'number',
        default: 3500,
      },
      {
        displayName: 'Overlap (chars)',
        name: 'overlap',
        type: 'number',
        default: 250,
      },
      {
        displayName: 'Split Method',
        name: 'splitMethod',
        type: 'options',
        options: [
          { name: 'heading-aware', value: 'heading' },
          { name: 'smart-sentence', value: 'smart' },
          { name: 'fixed', value: 'fixed' },
        ],
        default: 'heading',
      },
      // Memory
      {
        displayName: 'Memory Mode',
        name: 'memoryMode',
        type: 'options',
        options: [
          { name: 'Transient (in-run)', value: 'transient' },
          { name: 'Workflow static (persist)', value: 'workflowStatic' },
          { name: 'Google Drive (persist)', value: 'googleDrive' },
        ],
        default: 'workflowStatic',
      },
      {
        displayName: 'Memory Key',
        name: 'memoryKey',
        type: 'string',
        default: 'default-book',
      },
      // Stages collection (up to 10)
      {
        displayName: 'Stages',
        name: 'stages',
        type: 'fixedCollection',
        typeOptions: { multipleValues: true, minValue: 1, maxValue: 10 },
        placeholder: 'Add Stage',
        default: {},
        options: [
          {
            displayName: 'Stage',
            name: 'stageValues',
            values: [
              { displayName: 'Name', name: 'name', type: 'string', default: 'Stage' },
              { displayName: 'Enabled', name: 'enabled', type: 'boolean', default: true },
              { displayName: 'Prompt', name: 'prompt', type: 'string', typeOptions: { rows: 6 }, default: 'Edit the input: {{text}}' },
              { displayName: 'Model (optional)', name: 'model', type: 'string', default: '' },
              { displayName: 'Temperature', name: 'temperature', type: 'number', default: 0.05 },
              { displayName: 'Max tokens', name: 'maxTokens', type: 'number', default: 800 },
              { displayName: 'Output format', name: 'format', type: 'options', options: [{ name: 'text', value: 'text' }, { name: 'json', value: 'json' }], default: 'text' },
              { displayName: 'Condition (optional)', name: 'condition', type: 'string', default: '' },
              { displayName: 'Save stage output', name: 'saveOutput', type: 'boolean', default: false }
            ],
          },
        ],
      },
      // Output
      {
        displayName: 'Output Format',
        name: 'outputFormat',
        type: 'options',
        options: [{ name: 'TXT', value: 'txt' }, { name: 'JSON', value: 'json' }],
        default: 'txt',
      },
      {
        displayName: 'Verbose (Save raw OpenAI responses)',
        name: 'verbose',
        type: 'boolean',
        default: false,
      },
    ],
  };

  // ---------- Helper functions ----------
  private tailOf(text: string, maxChars = 400) {
    if (!text) return '';
    const s = text.slice(-maxChars);
    // find last sentence boundary near start of s
    const re = /[\.؟!\n]\s+/;
    const m = s.match(re);
    if (m) {
      const idx = s.indexOf(m[0]);
      if (idx >= 0 && idx + m[0].length < s.length) {
        return s.slice(idx + m[0].length).trim();
      }
    }
    return s.trim();
  }

  private reassemble(editedChunks: string[], overlapChars = 200) {
    if (!editedChunks || !editedChunks.length) return '';
    let result = editedChunks[0];
    for (let i = 1; i < editedChunks.length; i++) {
      const prev = result;
      const curr = editedChunks[i];
      let cut = 0;
      const maxCheck = Math.min(overlapChars, Math.floor(prev.length / 2), Math.floor(curr.length / 2));
      for (let k = maxCheck; k >= 20; k--) {
        if (prev.slice(-k) === curr.slice(0, k)) {
          cut = k;
          break;
        }
      }
      if (cut) result = prev + curr.slice(cut);
      else result = prev + '\n\n' + curr;
    }
    return result;
  }

  // load memory (workflowStatic or googleDrive)
  private async loadMemory(thisNode: IExecuteFunctions, memoryKey: string, mode: string) {
    if (mode === 'workflowStatic') {
      const wfStatic = thisNode.getWorkflowStaticData('global') as any;
      return wfStatic[`memory_${memoryKey}`] || null;
    }
    if (mode === 'googleDrive') {
      // requires googleDriveOAuth2Api credential in node
      // file name: openai_text_refinery_memory_<memoryKey>.json
      try {
        const filename = `openai_text_refinery_memory_${memoryKey}.json`;
        // use Drive API to search file
        const search = await thisNode.helpers.request!.call(thisNode, {
          method: 'GET',
          uri: `https://www.googleapis.com/drive/v3/files?q=name='${encodeURIComponent(filename)}' and trashed=false&fields=files(id,name)`,
          json: true,
        });
        if (search && search.files && search.files.length) {
          const fileId = search.files[0].id;
          const content = await thisNode.helpers.request!.call(thisNode, {
            method: 'GET',
            uri: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`,
            json: true,
          });
          return content;
        }
      } catch (e) {
        // ignore, return null
      }
      return null;
    }
    return null;
  }

  private async persistMemory(thisNode: IExecuteFunctions, memoryKey: string, memoryObj: any, mode: string) {
    memoryObj.lastUpdated = new Date().toISOString();
    memoryObj.version = (memoryObj.version || 0) + 1;
    if (mode === 'workflowStatic') {
      const wfStatic = thisNode.getWorkflowStaticData('global') as any;
      wfStatic[`memory_${memoryKey}`] = memoryObj;
      return true;
    }
    if (mode === 'googleDrive') {
      // upload or update file
      const filename = `openai_text_refinery_memory_${memoryKey}.json`;
      const content = JSON.stringify(memoryObj, null, 2);
      try {
        // find existing
        const search = await thisNode.helpers.request!.call(thisNode, {
          method: 'GET',
          uri: `https://www.googleapis.com/drive/v3/files?q=name='${encodeURIComponent(filename)}' and trashed=false&fields=files(id,name)`,
          json: true,
        });
        if (search && search.files && search.files.length) {
          const fileId = search.files[0].id;
          // update
          await thisNode.helpers.request!.call(thisNode, {
            method: 'PATCH',
            uri: `https://www.googleapis.com/upload/drive/v3/files/${fileId}?uploadType=media`,
            body: content,
            json: true,
            headers: { 'Content-Type': 'application/json' },
          });
        } else {
          // create
          // multipart upload (metadata + media)
          const metadata = { name: filename };
          const boundary = '-------314159265358979323846';
          const multipartRequestBody =
            `--${boundary}\r\n` +
            'Content-Type: application/json; charset=UTF-8\r\n\r\n' +
            JSON.stringify(metadata) + '\r\n' +
            `--${boundary}\r\n` +
            'Content-Type: application/json\r\n\r\n' +
            content + '\r\n' +
            `--${boundary}--`;
          await thisNode.helpers.request!.call(thisNode, {
            method: 'POST',
            uri: 'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart',
            body: multipartRequestBody,
            headers: { 'Content-Type': `multipart/related; boundary="${boundary}"` },
            json: true,
          });
        }
        return true;
      } catch (e) {
        return false;
      }
    }
    return false;
  }

  // call OpenAI Responses API via helpers.request (credential applied)
  private async callOpenAI(thisNode: IExecuteFunctions, model: string, content: string, temperature = 0.05, maxTokens = 800) {
    const body = {
      model,
      input: [{ role: 'user', content }],
      temperature,
      max_tokens: maxTokens,
    };
    const options = {
      method: 'POST',
      uri: 'https://api.openai.com/v1/responses',
      body,
      json: true,
    };
    // this.helpers.request will use the node credential (openAiAccountApi) automatically
    return thisNode.helpers.request!(options);
  }

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    if (items.length === 0) return this.prepareOutputData([]);
    // read parameters
    const inputField = this.getNodeParameter('inputField', 0) as string;
    const defaultModel = this.getNodeParameter('defaultModel', 0) as string;
    const defaultTemperature = this.getNodeParameter('defaultTemperature', 0) as number;
    const chunkSize = this.getNodeParameter('chunkSize', 0) as number;
    const overlap = this.getNodeParameter('overlap', 0) as number;
    const splitMethod = this.getNodeParameter('splitMethod', 0) as string;
    const memoryMode = this.getNodeParameter('memoryMode', 0) as string;
    const memoryKey = this.getNodeParameter('memoryKey', 0) as string;
    const stagesRaw = this.getNodeParameter('stages.stageValues', 0, []) as any[];
    const verbose = this.getNodeParameter('verbose', 0) as boolean;
    const outputFormat = this.getNodeParameter('outputFormat', 0) as string;

    // normalize stages
    const stages: StageConfig[] = (stagesRaw || []).map((s: any) => ({
      name: s.name || 'Stage',
      enabled: s.enabled !== false,
      prompt: s.prompt || '',
      model: s.model || '',
      temperature: s.temperature ?? defaultTemperature,
      maxTokens: s.maxTokens ?? 800,
      format: s.format || 'text',
      condition: s.condition || '',
      saveOutput: s.saveOutput || false,
    }));

    const returnData: INodeExecutionData[] = [];

    // --- process each incoming item (batch support) ---
    for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
      const item = items[itemIndex];
      let text = (item.json as any)[inputField] || '';
      // if binary content present, convert
      if (!text && item.binary) {
        const keys = Object.keys(item.binary);
        if (keys.length) {
          const b = item.binary[keys[0]];
          const buf = Buffer.from(b.data, b.encoding || 'base64');
          text = buf.toString('utf8');
        }
      }
      // load memory
      let currentMemory = (await this.loadMemory(this, memoryKey, memoryMode)) || {
        memoryId: memoryKey,
        version: 1,
        createdAt: new Date().toISOString(),
        lastUpdated: new Date().toISOString(),
        style_profile: { tone: '', formality: '', notes: '' },
        glossary: { term_map: {}, lastModified: new Date().toISOString() },
        context_summary: { short: '', long: '' },
        dynamic_summary: {},
        last_edited_tail: { text: '', length_chars: 0 },
        stage_metadata: {},
        usage_stats: { totalTokensUsed: 0, executions: 0 },
      };

      // chunking
      const chunks = this.chunkText(text, chunkSize, overlap, splitMethod);

      const editedChunks: string[] = [];
      const stageOutputsAll: any = {};

      // process chunks serially for best quality
      for (let c = 0; c < chunks.length; c++) {
        const chunkObj = chunks[c];
        let currentChunkText = chunkObj.chunk as string;

        // previousTail injection: prefer memory.last_edited_tail else chunkObj.previousTail
        let previousTail = currentMemory.last_edited_tail?.text || chunkObj.previousTail || '';

        // run each stage on the current chunk
        for (let sIndex = 0; sIndex < stages.length; sIndex++) {
          const st = stages[sIndex];
          if (!st.enabled) continue;
          // condition evaluation (simple check) - if condition present and false, skip
          if (st.condition && st.condition.trim()) {
            // At this level we don't evaluate complex expressions; assume true or do a basic JSON path check if needed
          }

          const modelToUse = st.model && st.model.length ? st.model : defaultModel;
          // build prompt
          const prompt = this.buildStagePrompt(st, currentChunkText, currentMemory, previousTail);

          // call OpenAI
          const resp = await this.callOpenAI(this, modelToUse, prompt, st.temperature, st.maxTokens || 800);

          // parse response -> try several shapes
          const edited = this.parseOpenAIResponse(resp);
          // if st.format == json we might keep JSON; here we keep plain string
          const editedText = typeof edited === 'string' ? edited : JSON.stringify(edited);

          // extract tail from editedText
          const tail = this.tailOf(editedText, Math.max(200, overlap));
          // update memory in-run
          currentMemory.last_edited_tail = { text: tail, length_chars: tail.length };
          // (Optionally) you could extract style/glossary suggestions via a micro-prompt and merge them here.

          // save stage output if requested
          if (!stageOutputsAll[`stage${sIndex + 1}`]) stageOutputsAll[`stage${sIndex + 1}`] = [];
          stageOutputsAll[`stage${sIndex + 1}`].push({ chunkIndex: c, output: editedText });

          // pass output to next stage
          currentChunkText = editedText;

          // accumulate usage stats (approx: we don't have exact token count here)
          currentMemory.usage_stats.totalTokensUsed = (currentMemory.usage_stats.totalTokensUsed || 0) + 0; // placeholder
        } // stages loop

        // after all stages for this chunk
        editedChunks.push(currentChunkText);
      } // chunks loop

      // reassemble final text
      const finalText = this.reassemble(editedChunks, overlap);

      // persist memory if requested (we persist by default for workflowStatic and googleDrive)
      if (memoryMode === 'workflowStatic' || memoryMode === 'googleDrive') {
        await this.persistMemory(this, memoryKey, currentMemory, memoryMode);
      }

      // prepare output
      const outItem: INodeExecutionData = {
        json: {
          originalText: text.slice(0, 2000),
          finalText,
          stageOutputs: stageOutputsAll,
          memory: currentMemory,
        },
      };
      if (verbose) outItem.json.rawDebug = {}; // could store raw responses if captured above
      returnData.push(outItem);
    } // items loop

    return this.prepareOutputData(returnData);
  }

  // --- small helpers used by execute: chunkText, buildStagePrompt, parseOpenAIResponse ---

  private chunkText(text: string, chunkSize = 3500, overlap = 250, method = 'heading') {
    // simple heading-aware chunker: try to cut at headings or paragraphs; fallback to fixed
    const out: { chunk: string; chunkIndex: number; previousTail?: string }[] = [];
    let pos = 0;
    let idx = 0;
    while (pos < text.length) {
      if (pos + chunkSize >= text.length) {
        out.push({ chunk: text.slice(pos), chunkIndex: idx++ });
        break;
      }
      let window = text.slice(pos, pos + chunkSize);
      // try heading regex
      const headingMatch = window.match(/(فصل\s*[:\-\s]?\s*[0-9۰-۹]+|CHAPTER\s+\d+|Chapter\s+\d+)/i);
      let cut = -1;
      if (method === 'heading' && headingMatch) {
        const mIndex = window.indexOf(headingMatch[0]);
        if (mIndex > 100) {
          cut = mIndex;
        }
      }
      if (cut < 0) {
        // natural cut: last double newline or last sentence end
        let posCut = window.lastIndexOf('\n\n');
        if (posCut >= Math.floor(chunkSize * 0.6)) cut = posCut;
        else {
          // sentence end punctuation
          const endings = ['. ', '? ', '! ', '؟ ', '؛ ', '.\n'];
          for (const e of endings) {
            const p = window.lastIndexOf(e);
            if (p >= Math.floor(chunkSize * 0.6)) {
              cut = p + e.length;
              break;
            }
          }
        }
      }
      if (cut < 0) cut = chunkSize;
      const piece = text.slice(pos, pos + cut);
      out.push({ chunk: piece, chunkIndex: idx++ });
      pos += cut;
      // apply overlap by moving back
      pos = Math.max(0, pos - overlap);
    }
    return out;
  }

  private buildStagePrompt(stage: StageConfig, chunkText: string, memory: any, previousTail: string) {
    // inject memory and previousTail at top
    const styleNotes = (memory && memory.style_profile && memory.style_profile.notes) ? memory.style_profile.notes : '';
    const glossary = memory && memory.glossary && memory.glossary.term_map ? Object.entries(memory.glossary.term_map).map(([k,v]) => `${k} = ${v}`).join('\n') : '';
    const contextShort = memory && memory.context_summary && memory.context_summary.short ? memory.context_summary.short : '';

    const promptParts = [
      '# MEMORY',
      'Style Profile:',
      styleNotes,
      '',
      'Glossary:',
      glossary,
      '',
      'Context Summary:',
      contextShort,
      '',
      'Last Edited Tail:',
      previousTail || '',
      '',
      '---',
      `You are performing: ${stage.name}`,
      'Rules: Preserve meaning, do not invent facts, keep tone consistent with Style Profile, preserve terminology in Glossary.',
      '',
      `Task: ${stage.prompt}`,
      '',
      'Input chunk:',
      chunkText,
      '',
      'Return ONLY the edited chunk (no commentary).'
    ];
    return promptParts.join('\n');
  }

  private parseOpenAIResponse(resp: any) {
    if (!resp) return '';
    // Responses API shapes vary; try common fields
    if (resp.output && Array.isArray(resp.output) && resp.output.length) {
      // often output[0].content or text
      const o = resp.output[0];
      if (typeof o === 'string') return o;
      if (o.content && Array.isArray(o.content) && o.content.length) {
        return o.content.map((c: any) => (typeof c === 'string' ? c : c.text || '')).join('');
      }
      if (o.text) return o.text;
    }
    if (resp.output_text) {
      return Array.isArray(resp.output_text) ? resp.output_text.join('') : resp.output_text;
    }
    if (resp.choices && Array.isArray(resp.choices) && resp.choices[0]) {
      if (resp.choices[0].message && resp.choices[0].message.content) {
        if (typeof resp.choices[0].message.content === 'string') return resp.choices[0].message.content;
        if (resp.choices[0].message.content.parts) return resp.choices[0].message.content.parts.join('');
      }
      if (resp.choices[0].text) return resp.choices[0].text;
    }
    try { return JSON.stringify(resp); } catch (e) { return String(resp); }
  }
}
