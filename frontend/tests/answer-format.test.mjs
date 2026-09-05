import test from "node:test";
import assert from "node:assert/strict";
import { formatAnswerInline } from "../.test-build/answer-format.js";

test("source markup cannot create active HTML or external citation links", () => {
  const output = formatAnswerInline('<img src=x onerror="alert(1)"> [bad](javascript:alert(1)) **bold**');
  assert.ok(output.includes("&lt;img"));
  assert.ok(!output.includes("<img"));
  assert.ok(!output.includes("<a"));
  assert.ok(output.includes("<strong>bold</strong>"));
});

test("only matching document ID citations become navigable links", () => {
  const output = formatAnswerInline("$321.00 [Document 101](/documents/101) [Document 7](/documents/8)");
  assert.ok(output.includes('href="/documents/101"'));
  assert.ok(!output.includes('href="/documents/8"'));
});
