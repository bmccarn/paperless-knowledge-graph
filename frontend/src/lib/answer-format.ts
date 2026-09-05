/** Escape source/model text before introducing our own limited markup. */
export function formatAnswerInline(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.+?)\*/g, "<em>$1</em>")
    .replace(/`(.+?)`/g, '<code class="bg-muted px-1.5 py-0.5 rounded text-xs font-mono">$1</code>')
    .replace(/\[Document (\d+)\]\(\/documents\/\1\)/g,
      '<a href="/documents/$1" class="text-primary underline underline-offset-2">Document $1</a>');
}
