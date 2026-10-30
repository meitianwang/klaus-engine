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

  function transform(source) {
    const parse = globalThis.__acornParse;
    const ast = parse(source, {
      sourceType: 'module',
      allowAwaitOutsideFunction: true,
      ecmaVersion: 'latest',
    });
    const declNames = [];
    let lastExprStmt = null;
    for (const stmt of ast.body) {
      collectStatementNames(stmt, declNames);
      if (stmt.type === 'ExpressionStatement') lastExprStmt = stmt;
    }
    const hasLastExpr = lastExprStmt !== null;
    let body;
    if (hasLastExpr) {
      const head = source.slice(0, lastExprStmt.start);
      const expr = source.slice(lastExprStmt.expression.start, lastExprStmt.expression.end);
      const tail = source.slice(lastExprStmt.end);
      body = `${head}__klausLast = (${expr});${tail}`;
    } else {
      body = source;
    }
    const declsExpr = declNames
      .map((name) => `${JSON.stringify(name)}: (() => { try { return ${name}; } catch { return undefined; } })()`)
      .join(', ');
    const wrapped =
      `(async () => {\nlet __klausLast = null;\n${body}\nreturn { last: __klausLast, decls: { ${declsExpr} } };\n})()`;
    return { wrapped, declNames, hasLastExpr };
  }

  globalThis.__klausTransform = transform;
})();
