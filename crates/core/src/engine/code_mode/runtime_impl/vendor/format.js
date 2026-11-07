// Klaus REPL result formatter: an inspect-equivalent subset for sealed JS
// runtimes that lack node:util. Parity target:
//   require('node:util').inspect(v, { colors: false, depth: 10, customInspect: false })
// Documented intentional deviations:
//  - No line wrapping: values whose util output would exceed breakLength are
//    rendered single-line.
//  - Settled promises render as 'Promise { <pending> }': promise state is not
//    synchronously observable without V8 internals.
//  - Multi-line wrapping is not reproduced: values whose util output would
//    break across lines (deep nesting, many array entries, over-wide single
//    entries) render single-line here. Parity is guaranteed on the
//    single-line region only.
//  - ArrayBuffer/boxed primitives are not part of the parity corpus.
(function () {
  'use strict';
  const DEPTH = 10;
  const IDENT_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/u;

  function quoteString(s) {
    let quote = "'";
    if (s.includes("'") && !s.includes('"')) quote = '"';
    else if (s.includes("'") && s.includes('"') && !s.includes('`') && !s.includes('${')) quote = '`';
    let out = '';
    for (let i = 0; i < s.length; i++) {
      const ch = s[i];
      const c = s.charCodeAt(i);
      if (ch === '\\') out += '\\\\';
      else if (ch === '\n') out += '\\n';
      else if (ch === '\r') out += '\\r';
      else if (ch === '\t') out += '\\t';
      else if (ch === '\b') out += '\\b';
      else if (ch === '\f') out += '\\f';
      else if (ch === '\v') out += '\\v';
      else if (c === 0) out += '\\x00';
      else if (quote === '`' && ch === '$' && s[i + 1] === '{') { out += '\\${'; i++; }
      else if (ch === quote) out += '\\' + ch;
      else if (c < 32 || c === 127) out += '\\x' + c.toString(16).padStart(2, '0');
      else if (c === 0x2028) out += '\\u2028';
      else if (c === 0x2029) out += '\\u2029';
      else out += ch;
    }
    return quote + out + quote;
  }

  function formatKey(k) { return IDENT_RE.test(k) ? k : quoteString(k); }

  function formatFunction(v) {
    const kind = v.constructor && v.constructor.name !== 'Function' && v.constructor.name ? v.constructor.name : 'Function';
    return `[${kind}${v.name ? ': ' + v.name : ' (anonymous)'}]`;
  }

  function placeholderFor(kind, v) {
    if (kind === 'array') return '[Array]';
    if (kind === 'map') return '[Map]';
    if (kind === 'set') return '[Set]';
    if (kind === 'dataview') return '[DataView]';
    if (kind === 'typed') return `[${v.constructor.name}]`;
    if (kind === 'promise') return '[Promise]';
    if (kind === 'date') return '[Date]';
    if (kind === 'regexp') return '[RegExp]';
    if (kind === 'error') return '[Error]';
    return '[Object]';
  }

  function kindOf(v) {
    if (Array.isArray(v)) return 'array';
    if (v instanceof Map) return 'map';
    if (v instanceof Set) return 'set';
    if (v instanceof Date) return 'date';
    if (v instanceof RegExp) return 'regexp';
    if (v instanceof Promise) return 'promise';
    if (v instanceof Error) return 'error';
    if (ArrayBuffer.isView(v)) return v instanceof DataView ? 'dataview' : 'typed';
    return 'object';
  }

  function wrap(parts, open, close) {
    return parts.length === 0 ? open + close : open + ' ' + parts.join(', ') + ' ' + close;
  }

  function fmt(v, level, ctx) {
    const t = typeof v;
    if (v === null) return 'null';
    if (t === 'undefined') return 'undefined';
    if (t === 'boolean') return v ? 'true' : 'false';
    if (t === 'number') return Object.is(v, -0) ? '-0' : String(v);
    if (t === 'bigint') return String(v) + 'n';
    if (t === 'symbol') return v.toString();
    if (t === 'string') return quoteString(v);
    if (t === 'function') return formatFunction(v);

    const kind = kindOf(v);
    if (ctx.ancestors.has(v)) {
      let id = ctx.ids.get(v);
      if (id === undefined) { id = ctx.next++; ctx.ids.set(v, id); }
      return `[Circular *${id}]`;
    }
    const seenId = ctx.ids.get(v);
    const prefix = seenId !== undefined ? `<ref *${seenId}> ` : '';
    if (level > DEPTH) return prefix + placeholderFor(kind, v);
    if (kind === 'error') return typeof v.stack === 'string' ? v.stack : `${v.name}: ${v.message}`;
    if (kind === 'date') {
      const time = Date.prototype.getTime.call(v);
      return prefix + (Number.isNaN(time) ? 'Invalid Date' : new Date(time).toISOString());
    }
    if (kind === 'regexp') return prefix + '/' + v.source + '/' + v.flags;
    if (kind === 'promise') return prefix + 'Promise { <pending> }';

    ctx.ancestors.add(v);
    let body;
    if (kind === 'array') {
      const parts = [];
      let holes = 0;
      const flushHoles = () => { if (holes > 0) { parts.push(`<${holes} empty item${holes > 1 ? 's' : ''}>`); holes = 0; } };
      for (let i = 0; i < v.length; i++) {
        if (!Object.prototype.hasOwnProperty.call(v, i)) { holes++; continue; }
        flushHoles();
        parts.push(fmt(v[i], level + 1, ctx));
      }
      flushHoles();
      for (const key of Object.keys(v)) {
        if (/^(0|[1-9][0-9]*)$/.test(key) && Number(key) < v.length) continue;
        parts.push(`${formatKey(key)}: ${fmt(v[key], level + 1, ctx)}`);
      }
      body = wrap(parts, '[', ']');
    } else if (kind === 'map') {
      const parts = [];
      for (const [k, val] of Map.prototype.entries.call(v)) parts.push(`${fmt(k, level + 1, ctx)} => ${fmt(val, level + 1, ctx)}`);
      body = `Map(${v.size}) ` + wrap(parts, '{', '}');
    } else if (kind === 'set') {
      const parts = [];
      for (const val of Set.prototype.values.call(v)) parts.push(fmt(val, level + 1, ctx));
      body = `Set(${v.size}) ` + wrap(parts, '{', '}');
    } else if (kind === 'typed' || kind === 'dataview') {
      if (kind === 'dataview') {
        body = `DataView { byteLength: ${v.byteLength} }`;
      } else {
        const parts = [];
        for (let i = 0; i < v.length; i++) parts.push(fmt(v[i], level + 1, ctx));
        body = `${v.constructor.name}(${v.length}) ` + wrap(parts, '[', ']');
      }
    } else {
      const parts = [];
      for (const key of Object.keys(v)) {
        const desc = Object.getOwnPropertyDescriptor(v, key);
        let text;
        if (desc.get || desc.set) text = desc.get && desc.set ? '[Getter/Setter]' : desc.get ? '[Getter]' : '[Setter]';
        else text = fmt(desc.value, level + 1, ctx);
        parts.push(`${formatKey(key)}: ${text}`);
      }
      let tag = '';
      const proto = Object.getPrototypeOf(v);
      if (proto === null) tag = '[Object: null prototype] ';
      else if (v.constructor && v.constructor.name && v.constructor.name !== 'Object') tag = v.constructor.name + ' ';
      body = tag + wrap(parts, '{', '}');
    }
    ctx.ancestors.delete(v);
    return prefix + body;
  }

  globalThis.__klausFormat = function (value) {
    const ctx = { ids: new Map(), next: 1, ancestors: new Set() };
    const out = fmt(value, 0, ctx);
    const id = ctx.ids.get(value);
    if (id !== undefined && !out.startsWith('<ref')) return `<ref *${id}> ${out}`;
    return out;
  };

  globalThis.__klausFormatError = function (err) {
    if (!(err instanceof Error)) return String(err);
    if (typeof err.stack !== 'string' || err.stack === '') return String(err.message);
    const lines = err.stack.split('\n');
    const head = lines[0] || String(err.message);
    const frames = lines.slice(1).filter((line) => line.trim().startsWith('at '));
    if (frames.length <= 5) return err.stack;
    return [head, ...frames.slice(0, 5)].join('\n');
  };
})();
