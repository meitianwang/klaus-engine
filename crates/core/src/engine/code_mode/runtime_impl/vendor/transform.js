// Klaus REPL top-level-await transform for sealed JS runtimes.
// Contract: globalThis.__acornParse must be acorn's parse function before use.
// __klausTransform(source) -> { wrapped, declNames, hasLastExpr }
//   wrapped: an async IIFE whose resolution value is
//     { last, decls } where last is the trailing expression value (or null)
//     and decls maps every top-level declaration name to its settled value.
// The host evaluates `wrapped` only when plain Script compilation fails on
// top-level await, then hoists decls onto the persistent context globals and
// uses `last` as the REPL completion value.
(function () {
  'use strict';

  function collectPatternNames(node, out) {
    if (!node) return;
    switch (node.type) {
      case 'Identifier':
        out.push(node.name);
        break;
      case 'ObjectPattern':
        for (const prop of node.properties) {
          if (prop.type === 'RestElement') collectPatternNames(prop.argument, out);
          else collectPatternNames(prop.value, out);
        }
        break;
      case 'ArrayPattern':
        for (const element of node.elements) collectPatternNames(element, out);
        break;
      case 'RestElement':
        collectPatternNames(node.argument, out);
        break;
      case 'AssignmentPattern':
        collectPatternNames(node.left, out);
        break;
      default:
        break;
    }
  }

  function collectStatementNames(stmt, out) {
    switch (stmt.type) {
      case 'VariableDeclaration':
        for (const decl of stmt.declarations) collectPatternNames(decl.id, out);
        break;
      case 'FunctionDeclaration':
      case 'ClassDeclaration':
        if (stmt.id) out.push(stmt.id.name);
        break;
      case 'ForOfStatement':
      case 'ForInStatement':
        if (stmt.left && stmt.left.type === 'VariableDeclaration') {
          for (const decl of stmt.left.declarations) collectPatternNames(decl.id, out);
        }
        break;
      case 'ForStatement':
        if (stmt.init && stmt.init.type === 'VariableDeclaration') {
          for (const decl of stmt.init.declarations) collectPatternNames(decl.id, out);
        }
        break;
      default:
        break;
    }
  }

  function collectReturns(stmt, out) {
    if (!stmt) return;
    switch (stmt.type) {
      case 'ReturnStatement':
        out.push(stmt);
        break;
      case 'Program':
      case 'BlockStatement':
        for (const child of stmt.body) collectReturns(child, out);
        break;
      case 'IfStatement':
        collectReturns(stmt.consequent, out);
        collectReturns(stmt.alternate, out);
        break;
      case 'SwitchStatement':
        for (const branch of stmt.cases) {
          for (const child of branch.consequent) collectReturns(child, out);
        }
        break;
      case 'TryStatement':
        collectReturns(stmt.block, out);
        collectReturns(stmt.handler?.body, out);
        collectReturns(stmt.finalizer, out);
        break;
      case 'ForStatement':
      case 'ForInStatement':
      case 'ForOfStatement':
      case 'WhileStatement':
      case 'DoWhileStatement':
      case 'WithStatement':
      case 'LabeledStatement':
        collectReturns(stmt.body, out);
        break;
    }
  }

  function transform(source) {
    const parse = globalThis.__acornParse;
    const ast = parse(source, {
      sourceType: 'module',
      allowAwaitOutsideFunction: true,
      allowReturnOutsideFunction: true,
      ecmaVersion: 'latest',
    });
    const declNames = [];
    let lastExprStmt = null;
    for (const stmt of ast.body) {
      collectStatementNames(stmt, declNames);
      if (stmt.type === 'ExpressionStatement') lastExprStmt = stmt;
    }
    const hasLastExpr = lastExprStmt !== null;
    const returns = [];
    collectReturns(ast, returns);
    const edits = returns.map((stmt) => {
      const arg = stmt.argument;
      const value = arg ? source.slice(arg.start, arg.end) : 'undefined';
      return { start: stmt.start, end: stmt.end,
        text: `return { last: (${value}), decls: __klausDecls() };` };
    });
    if (lastExprStmt) {
      const expr = lastExprStmt.expression;
      edits.push({ start: lastExprStmt.start, end: lastExprStmt.end,
        text: `__klausLast = (${source.slice(expr.start, expr.end)});` });
    }
    let body = source;
    for (const { start, end, text } of edits.sort((a, b) => b.start - a.start)) {
      body = body.slice(0, start) + text + body.slice(end);
    }
    const declsExpr = declNames
      .map((name) => `${JSON.stringify(name)}: (() => { try { return ${name}; } catch { return undefined; } })()`)
      .join(', ');
    const wrapped =
      `(async () => {\nlet __klausLast = null;\nconst __klausDecls = () => ({ ${declsExpr} });\n${body}\nreturn { last: __klausLast, decls: __klausDecls() };\n})()`;
    return { wrapped, declNames, hasLastExpr };
  }

  globalThis.__klausTransform = transform;
})();
