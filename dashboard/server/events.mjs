// reviver 的 source 是原始数字文本；不使用已被 Number 舍入的值来还原 u64。
export function parseRecordedJson(raw) {
  return JSON.parse(raw, (_key, value, context) =>
    typeof value === 'number' && Math.abs(value) > Number.MAX_SAFE_INTEGER ? context.source : value)
}

export function compareEvents(a, b) {
  return a.unix_ms - b.unix_ms || a.line - b.line
}

export function parseEventLog(content, lineOffset = 0) {
  const events = []
  const issues = []
  for (const [index, raw] of content.split(/\r?\n/).entries()) {
    if (!raw.trim()) continue
    const line = lineOffset + index + 1
    try {
      const entry = parseRecordedJson(raw)
      if (!entry || typeof entry !== 'object' || Array.isArray(entry) ||
          typeof entry.event !== 'string' || !entry.event.trim() ||
          !Number.isSafeInteger(entry.unix_ms) || entry.unix_ms < 0 ||
          !entry.fields || typeof entry.fields !== 'object' || Array.isArray(entry.fields)) {
        throw new Error('事件应包含非空 event、安全整数 unix_ms 和对象 fields')
      }
      events.push({ line, event: entry.event, unix_ms: entry.unix_ms, fields: entry.fields, raw })
    } catch (error) {
      issues.push({ line, message: error.message, raw })
    }
  }
  // 同一毫秒的事件保持文件顺序；不因名称或内容相同而去重。
  events.sort(compareEvents)
  return { events, issues }
}
