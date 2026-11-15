"use client";

import { useRef, useState } from "react";
import { MessageSquare, Pencil, Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { AlertDialog, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog";

interface Conversation { id: string; title: string }
export function ConversationList({ conversations, activeId, onNew, onOpen, onRename, onDelete }: {
  conversations: Conversation[]; activeId: string | null; onNew: () => void;
  onOpen: (conversation: Conversation) => void; onRename: (id: string, title: string) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
}) {
  const [editing, setEditing] = useState<string | null>(null);
  const [title, setTitle] = useState("");
  const [deleting, setDeleting] = useState<Conversation | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const renameTrigger = useRef<HTMLButtonElement | null>(null);
  const deleteTrigger = useRef<HTMLButtonElement | null>(null);
  const newButton = useRef<HTMLButtonElement | null>(null);
  const saving = useRef(false);
  async function save(id: string) {
    if (saving.current) return;
    if (!title.trim()) { setEditing(null); return; }
    saving.current = true;
    try { await onRename(id, title.trim()); setEditing(null); setError(""); renameTrigger.current?.focus(); }
    catch { setError("The conversation could not be renamed. Try again."); }
    finally { saving.current = false; }
  }
  return <>
    <div className="p-2"><Button ref={newButton} variant="outline" className="w-full" onClick={onNew}><Plus />New conversation</Button></div>
    <div className="flex-1 overflow-y-auto overflow-x-hidden" data-conversation-scroll>
      {error && !deleting && <p role="alert" className="p-3 text-sm text-error">{error}</p>}
      <ul aria-label="Saved conversations" className="space-y-1 px-2 pb-2">
        {conversations.map(conversation => <li key={conversation.id} className={`flex items-center rounded-md ${activeId === conversation.id ? "bg-raised" : "hover:bg-raised"}`}>
          {editing === conversation.id ? <Input autoFocus aria-label="Conversation title" className="min-w-0 flex-1"
            value={title} onChange={event => setTitle(event.target.value)} onBlur={event => { if (event.relatedTarget !== renameTrigger.current) void save(conversation.id); }}
            onKeyDown={event => { if (event.key === "Enter") { event.preventDefault(); void save(conversation.id); } if (event.key === "Escape") { setEditing(null); renameTrigger.current?.focus(); } }} /> :
            <button aria-current={activeId === conversation.id ? "true" : undefined} className="flex min-h-11 min-w-0 flex-1 items-center gap-2 px-2 text-left text-sm md:min-h-9"
              onClick={() => onOpen(conversation)}><MessageSquare aria-hidden="true" className="size-4 shrink-0 text-fg-muted" /><span className="truncate">{conversation.title}</span></button>}
          <Button variant="ghost" size="icon" aria-label={`Rename ${conversation.title}`} title="Rename"
            onClick={event => { renameTrigger.current = event.currentTarget; setEditing(conversation.id); setTitle(conversation.title); }}><Pencil /></Button>
          <Button variant="ghost" size="icon" aria-label={`Delete ${conversation.title}`} title="Delete" className="text-fg-muted hover:text-error"
            onClick={event => { deleteTrigger.current = event.currentTarget; setError(""); setDeleting(conversation); }}><Trash2 /></Button>
        </li>)}
      </ul>
      {!conversations.length && <div className="space-y-3 px-4 py-6 text-center text-sm text-fg-muted"><p>No conversations yet.</p><Button variant="outline" onClick={onNew}>Ask a question</Button></div>}
    </div>
    <AlertDialog open={!!deleting} onOpenChange={open => { if (!open && !pending) { setDeleting(null); setError(""); } }}>
      <AlertDialogContent onCloseAutoFocus={event => { event.preventDefault(); ([deleteTrigger.current, newButton.current].find(element => element?.isConnected) ?? document.getElementById("question"))?.focus(); }}>
        <AlertDialogHeader><AlertDialogTitle>Delete conversation?</AlertDialogTitle><AlertDialogDescription>“{deleting?.title}” and its messages will be permanently deleted.</AlertDialogDescription></AlertDialogHeader>
        {error && <p role="alert" className="text-sm text-error">{error}</p>}
        <AlertDialogFooter><AlertDialogCancel disabled={pending}>Cancel</AlertDialogCancel><Button variant="destructive" disabled={pending} onClick={async () => {
          if (!deleting) return;
          setPending(true);
          try { await onDelete(deleting.id); setDeleting(null); setError(""); }
          catch { setError("The conversation could not be deleted. Try again."); }
          finally { setPending(false); }
        }}>{pending ? "Deleting…" : "Delete conversation"}</Button></AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  </>;
}
