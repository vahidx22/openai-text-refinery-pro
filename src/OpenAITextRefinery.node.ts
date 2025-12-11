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

type MemoryShape = {
  memoryId?: string;
  version: number;
  createdAt?: string;
  lastUpdated: string;
  style_profile: {
    tone?: string;
    formality?: string;
    notes?: string;
  };
  glossary: {
    term_map: Record<string, string>;
    lastModified: string;
  };
  context_summary: {
    short?: string;
    long?: string;
  };
  dynamic_summary?: any;
  last_edited_tail: {
    text: string;
    length_chars: number;
  };
  stage_metadata?: any;
  usage_stats: {
    totalTokensUsed: number;
    executions: number;
  };
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
        name: 'openAiApi',
        required: true,
      },
      {
        name: 'googleDriveOAuth2Api',
        required: false,
      },
    ],
    properties: [
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
        options: [
          { name: 'gpt-4', value: 'gpt-4' },
          { name: 'gpt-4-turbo', value: 'gpt-4-turbo' },
          { name: 'gpt-3.5-turbo', value: 'gpt-3.5-turbo' },
        ],
        default: 'gpt-4-turbo',
      },
      {
        displayName: 'Default Temperature',
        name: 'defaultTemperature',
        type: 'number',
        default: 0.05,
        typeOptions: {
          minValue: 0,
          maxValue: 2,
          numberPrecision: 2,
        },
      },
      {
        displayName: 'Chunk Size (chars)',
        name: 'chunkSize',
        type: 'number',
        default: 3500,
        description: 'Maximum size of each text chunk',
      },
      {
        displayName: 'Overlap (chars)',
        name: 'overlap',
        type: 'number',
        default: 250,
        description: 'Number of characters to overlap between chunks',
      },
      {
        displayName: 'Split Method',
        name: 'splitMethod',
        type: 'options',
        options: [
          { name: 'Heading-aware', value: 'heading' },
          { name: 'Smart-sentence', value: 'smart' },
          { name: 'Fixed', value: 'fixed' },
        ],
        default: 'heading',
      },
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
        description: 'Unique identifier for this memory context',
      },
      {
        displayName: 'Stages',
        name: 'stages',
        type: 'fixedCollection',
        typeOptions: {
          multipleValues: true,
        },
        placeholder: 'Add Stage',
        default: {},
        options: [
          {
            displayName: 'Stage',
            name: 'stageValues',
            values: [
              {
                displayName: 'Name',
                name: 'name',
                type: 'string',
                default: 'Stage',
              },
              {
                displayName: 'Enabled',
                name: 'enabled',
                type: 'boolean',
                default: true,
              },
              {
                displayName: 'Prompt',
                name: 'prompt',
                type: 'string',
                typeOptions: { rows: 6 },
                default: 'Edit the input: {{text}}',
              },
              {
                displayName: 'Model (optional)',
                name: 'model',
                type: 'string',
                default: '',
              },
              {
                displayName: 'Temperature',
                name: 'temperature',
                type: 'number',
                default: 0.05,
              },
              {
                displayName: 'Max Tokens',
                name: 'maxTokens',
                type: 'number',
                default: 800,
              },
              {
                displayName: 'Output Format',
                name: 'format',
                type: 'options',
                options: [
                  { name: 'Text', value: 'text' },
                  { name: 'JSON', value: 'json' },
                ],
                default: 'text',
              },
              {
                displayName: 'Condition (optional)',
                name: 'condition',
                type: 'string',
                default: '',
              },
              {
                displayName: 'Save Stage Output',
                name: 'saveOutput',
                type: 'boolean',
                default: false,
              },
            ],
          },
        ],
      },
      {
        displayName: 'Output Format',
        name: 'outputFormat',
        type: 'options',
        options: [
          { name: 'TXT', value: 'txt' },
          { name: 'JSON', value: 'json' },
        ],
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
  
  private tailOf(text: string, maxChars = 400): string {
    if (!text) return '';
    const s = text.slice(-maxChars);
    const re = /[\.؟!\n]\s+/;
    const m = s.match(re);
    if (m && m.index !== undefined) {
      const idx = m.index;
      if (idx >= 0 && idx + m[0].length < s.length) {
        return s.slice(idx + m[0].length).trim();
      }
    }
    return s.trim();
  }

  private reassemble(editedChunks: string[], overlapChars = 200): string {
    if (!editedChunks || !editedChunks.length) return '';
    let result = editedChunks[0];
    
    for (let i = 1; i < editedChunks.length; i++) {
      const prev = result;
      const curr = editedChunks[i];
      let cut = 0;
      const maxCheck = Math.min(
        overlapChars,
        Math.floor(prev.length / 2),
        Math.floor(curr.length / 2)
      );
      
      for (let k = maxCheck; k >= 20; k--) {
        if (prev.slice(-k) === curr.slice(0, k)) {
          cut = k;
          break;
        }
      }
      
      if (cut) {
        result = prev + curr.slice(cut);
      } else {
        result = prev + '\n\n' + curr;
      }
    }
    return result;
  }

  private async loadMemory(
    thisNode: IExecuteFunctions,
    memoryKey: string,
    mode: string
  ): Promise<MemoryShape | null> {
    if (mode === 'workflowStatic') {
      const wfStatic = thisNode.getWorkflowStaticData('global') as any;
      return wfStatic[`memory_${memoryKey}`] || null;
    }
    
    if (mode === 'googleDrive') {
      try {
        const filename = `openai_text_refinery_memory_${memoryKey}.json`;
        const search = await thisNode.helpers.httpRequestWithAuthentication.call(
          thisNode,
          'googleDriveOAuth2Api',
          {
            method: 'GET',
            url: `https://www.googleapis.com/drive/v3/files?q=name='${encodeURIComponent(
              filename
            )}' and trashed=false&fields=files(id,name)`,
            json: true,
          }
        );
        
        if (search && search.files && search.files.length) {
          const fileId = search.files[0].id;
          const content = await thisNode.helpers.httpRequestWithAuthentication.call(
            thisNode,
            'googleDriveOAuth2Api',
            {
              method: 'GET',
              url: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`,
              json: true,
            }
          );
          return content as MemoryShape;
        }
      } catch (e) {
        // Ignore error, return null
      }
      return null;
    }
    
    return null;
  }

  private async persistMemory(
    thisNode: IExecuteFunctions,
    memoryKey: string,
    memoryObj: MemoryShape,
    mode: string
  ): Promise<boolean> {
    memoryObj.lastUpdated = new Date().toISOString();
    memoryObj.version = (memoryObj.version || 0) + 1;
    
    if (mode === 'workflowStatic') {
      const wfStatic = thisNode.getWorkflowStaticData('global') as any;
      wfStatic[`memory_${memoryKey}`] = memoryObj;
      return true;
    }
    
    if (mode === 'googleDrive') {
      const filename = `openai_text_refinery_memory_${memoryKey}.json`;
      const content = JSON.stringify(memoryObj, null, 2);
      
      try {
        const search = await thisNode.helpers.httpRequestWithAuthentication.call(
          thisNode,
          'googleDriveOAuth2Api',
          {
            method: 'GET',
            url: `https://www.googleapis.com/drive/v3/files?q=name='${encodeURIComponent(
              filename
            )}' and trashed=false&fields=files(id,name)`,
            json: true,
          }
        );
        
        if (search && search.files && search.files.length) {
          const fileId = search.files[0].id;
          await thisNode.helpers.httpRequestWithAuthentication.call(
            thisNode,
            'googleDriveOAuth2Api',
            {
              method: 'PATCH',
              url: `https://www.googleapis.com/upload/drive/v3/files/${fileId}?uploadType=media`,
              body: content,
              headers: { 'Content-Type': 'application/json' },
            }
          );
        } else {
          const metadata = { name: filename };
          const boundary = '-------314159265358979323846';
          const multipartRequestBody =
            `--${boundary}\r\n` +
            'Content-Type: application/json; charset=UTF-8\r\n\r\n' +
            JSON.stringify(metadata) +
            '\r\n' +
            `--${boundary}\r\n` +
            'Content-Type: application/json\r\n\r\n' +
            content +
            '\r\n' +
            `--${boundary}--`;
            
          await thisNode.helpers.httpRequestWithAuthentication.call(
            thisNode,
            'googleDriveOAuth2Api',
            {
              method: 'POST',
              url: 'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart',
              body: multipartRequestBody,
              headers: {
                'Content-Type': `multipart/related; boundary="${boundary}"`,
              },
            }
          );
        }
        return true;
      } catch (e) {
        return false;
      }
    }
    
    return false;
  }

  private async callOpenAI(
    thisNode: IExecuteFunctions,
    model: string,
    content: string,
    temperature = 0.05,
    maxTokens = 800
  ): Promise<any> {
    const body = {
      model,
      messages: [{ role: 'user', content }],
      temperature,
      max_tokens: maxTokens,
    };
    
    return await thisNode.helpers.httpRequestWithAuthentication.call(
      thisNode,
      'openAiApi',
      {
        method: 'POST',
        url: 'https://api.openai.com/v1/chat/completions',
        body,
        json: true,
      }
    );
  }

  private chunkText(
    text: string,
    chunkSize = 3500,
    overlap = 250,
    method = 'heading'
  ): Array<{ chunk: string; chunkIndex: number; previousTail?: string }> {
    const out: Array<{ chunk: string; chunkIndex: number; previousTail?: string }> = [];
    let pos = 0;
    let idx = 0;
    
    while (pos < text.length) {
      if (pos + chunkSize >= text.length) {
        out.push({ chunk: text.slice(pos), chunkIndex: idx++ });
        break;
      }
      
      let window = text.slice(pos, pos + chunkSize);
      const headingMatch = window.match(
        /(فصل\s*[:\-\s]?\s*[0-9۰-۹]+|CHAPTER\s+\d+|Chapter\s+\d+)/i
      );
      
      let cut = -1;
      if (method === 'heading' && headingMatch && headingMatch.index !== undefined) {
        const mIndex = headingMatch.index;
        if (mIndex > 100) {
          cut = mIndex;
        }
      }
      
      if (cut < 0) {
        let posCut = window.lastIndexOf('\n\n');
        if (posCut >= Math.floor(chunkSize * 0.6)) {
          cut = posCut;
        } else {
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
      pos = Math.max(0, pos - overlap);
    }
    
    return out;
  }

  private buildStagePrompt(
    stage: StageConfig,
    chunkText: string,
    memory: MemoryShape,
    previousTail: string
  ): string {
    const styleNotes = memory?.style_profile?.notes || '';
    const glossary = memory?.glossary?.term_map
      ? Object.entries(memory.glossary.term_map)
          .map(([k, v]) => `${k} = ${v}`)
          .join('\n')
      : '';
    const contextShort = memory?.context_summary?.short || '';

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
      'Return ONLY the edited chunk (no commentary).',
    ];
    
    return promptParts.join('\n');
  }

  private parseOpenAIResponse(resp: any): string {
    if (!resp) return '';
    
    // Standard chat completions format
    if (resp.choices && Array.isArray(resp.choices) && resp.choices[0]) {
      if (resp.choices[0].message?.content) {
        return resp.choices[0].message.content;
      }
      if (resp.choices[0].text) {
        return resp.choices[0].text;
      }
    }
    
    try {
      return JSON.stringify(resp);
    } catch (e) {
      return String(resp);
    }
  }

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    if (items.length === 0) return [[]];

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

    for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
      const item = items[itemIndex];
      let text = (item.json as any)[inputField] || '';

      if (!text && item.binary) {
        const keys = Object.keys(item.binary);
        if (keys.length) {
          const b = item.binary[keys[0]];
          const buf = Buffer.from(b.data, 'base64');
          text = buf.toString('utf8');
        }
      }

      let currentMemory: MemoryShape =
        (await this.loadMemory(this, memoryKey, memoryMode)) || {
          version: 1,
          createdAt: new Date().toISOString(),
          lastUpdated: new Date().toISOString(),
          style_profile: { tone: '', formality: '', notes: '' },
          glossary: { term_map: {}, lastModified: new Date().toISOString() },
          context_summary: { short: '', long: '' },
          last_edited_tail: { text: '', length_chars: 0 },
          usage_stats: { totalTokensUsed: 0, executions: 0 },
        };

      const chunks = this.chunkText(text, chunkSize, overlap, splitMethod);
      const editedChunks: string[] = [];
      const stageOutputsAll: any = {};

      for (let c = 0; c < chunks.length; c++) {
        const chunkObj = chunks[c];
        let currentChunkText = chunkObj.chunk;
        const previousTail = currentMemory.last_edited_tail?.text || '';

        for (let sIndex = 0; sIndex < stages.length; sIndex++) {
          const st = stages[sIndex];
          if (!st.enabled) continue;

          const modelToUse = st.model && st.model.length ? st.model : defaultModel;
          const prompt = this.buildStagePrompt(st, currentChunkText, currentMemory, previousTail);

          const resp = await this.callOpenAI(
            this,
            modelToUse,
            prompt,
            st.temperature,
            st.maxTokens || 800
          );

          const editedText = this.parseOpenAIResponse(resp);
          const tail = this.tailOf(editedText, Math.max(200, overlap));
          currentMemory.last_edited_tail = { text: tail, length_chars: tail.length };

          if (!stageOutputsAll[`stage${sIndex + 1}`]) {
            stageOutputsAll[`stage${sIndex + 1}`] = [];
          }
          stageOutputsAll[`stage${sIndex + 1}`].push({
            chunkIndex: c,
            output: editedText,
          });

          currentChunkText = editedText;
        }

        editedChunks.push(currentChunkText);
      }

      const finalText = this.reassemble(editedChunks, overlap);

      if (memoryMode === 'workflowStatic' || memoryMode === 'googleDrive') {
        await this.persistMemory(this, memoryKey, currentMemory, memoryMode);
      }

      const outItem: INodeExecutionData = {
        json: {
          originalText: text.slice(0, 2000),
          finalText,
          chunksCount: chunks.length,
          stageOutputs: verbose ? stageOutputsAll : undefined,
          memoryVersion: currentMemory.version,
        },
      };

      returnData.push(outItem);
    }

    return [returnData];
  }
}
