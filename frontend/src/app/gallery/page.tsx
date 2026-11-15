"use client";

import { useState, type ReactNode } from "react";
import { TabBar } from "@/components/sidebar-nav";
import { EvidenceLevel, StatusLabel } from "@/components/status-label";
import { KindShape, TypeDot } from "@/components/kind-shape";
import type { AnswerState, ClaimResult } from "@/lib/answer-state";
import { DOCUMENT_TYPE_KEYS, NODE_LABELS } from "@/lib/categories";
import { ArrowDown, ArrowUp, Check, FileText, Plus } from "lucide-react";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Collapsible, CollapsibleTrigger, CollapsibleContent } from "@/components/ui/collapsible";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogTrigger, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter, DialogClose } from "@/components/ui/dialog";
import { Sheet, SheetTrigger, SheetContent, SheetHeader, SheetTitle, SheetDescription, SheetFooter, SheetClose } from "@/components/ui/sheet";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell, TableCaption } from "@/components/ui/table";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Pager } from "@/components/pager";
import { AlertDialog, AlertDialogTrigger, AlertDialogContent, AlertDialogHeader, AlertDialogTitle, AlertDialogDescription, AlertDialogFooter, AlertDialogCancel, AlertDialogAction } from "@/components/ui/alert-dialog";
import { Toast } from "@/components/ui/toast";

const variants = ["default", "secondary", "outline", "ghost", "destructive", "link"] as const;
const sizes = ["xs", "sm", "default", "lg"] as const;
const documents = ["Annual statement", "Home inventory", "Service agreement"];

const answerStates: AnswerState[] = ["verified", "documented", "partial", "withheld", "not_checked", "retry", "failed"];
const claimResults: ClaimResult[] = ["supported", "unsupported", "conflicting", "left_out"];
const otherLabel = "Vehicle";

function Section({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <section aria-labelledby={id} className="space-y-3 border-t border-border pt-4">
      <h2 id={id} className="heading-16">{title}</h2>
      {children}
    </section>
  );
}

function Row({ name, children }: { name: string; children: ReactNode }) {
  return (
    <div className="grid gap-2 md:grid-cols-[160px_1fr] md:items-center">
      <p className="label-12 text-fg-muted">{name}</p>
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2">{children}</div>
    </div>
  );
}

export default function GalleryPage() {
  const [loading, setLoading] = useState(false);
  const [descending, setDescending] = useState(false);
  const [selected, setSelected] = useState<string | null>("Home inventory");
  const [page, setPage] = useState(0);
  return (
    <div className="h-full overflow-y-auto" data-gallery-scroll>
      <div className="mx-auto max-w-6xl space-y-10 p-4 md:px-12 md:py-10" data-gallery-content>
        <header className="space-y-2">
          <h1 className="page-title">Component gallery</h1>
          <p className="text-sm text-fg-muted">The shared controls for reading, reviewing and exploring your archive.</p>
        </header>

        <Section id="inspection-controls" title="Document inspection">
          <Row name="Alerts">{(["default", "warning", "destructive", "success"] as const).map(variant => <Alert key={variant} variant={variant}><AlertTitle>{variant === "default" ? "Information" : variant === "destructive" ? "Error" : variant === "warning" ? "Warning" : "Success"}</AlertTitle><AlertDescription>Review the original source before resolving this report.</AlertDescription></Alert>)}</Row>
          <Row name="Resolution"><Select defaultValue="reviewed"><SelectTrigger aria-label="Example resolution" className="max-w-sm"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="reviewed">Reindexed and checked correction</SelectItem><SelectItem value="dismissed">Checked original extraction; no correction needed</SelectItem></SelectContent></Select></Row>
          <Row name="Disclosure"><Collapsible><CollapsibleTrigger asChild><Button variant="outline">Original source text</Button></CollapsibleTrigger><CollapsibleContent className="ocr pt-3">The January statement lists a premium of $25.</CollapsibleContent></Collapsible></Row>
        </Section>

        <Section id="phone-navigation" title="Phone navigation">
          <div className="max-w-[390px] overflow-hidden rounded-lg border">
            <TabBar pathname="/query" healthy={true} className="static block md:block border-t-0" />
          </div>
        </Section>

        <Section id="answer-states" title="Answer states">
          <Row name="Medium">{answerStates.map(state => <StatusLabel key={state} status={state} />)}</Row>
          <Row name="Small">{answerStates.map(state => <StatusLabel key={state} status={state} size="sm" />)}</Row>
        </Section>

        <Section id="claim-results" title="Claim results">
          <Row name="Medium">{claimResults.map(result => <StatusLabel key={result} status={result} />)}</Row>
          <Row name="Small">{claimResults.map(result => <StatusLabel key={result} status={result} size="sm" />)}</Row>
        </Section>

        <Section id="evidence-levels" title="Evidence levels">
          <Row name="Level">{(["high", "medium", "low"] as const).map(level => <EvidenceLevel key={level} level={level} />)}</Row>
        </Section>

        <Section id="node-kinds" title="Entity kinds">
          <Row name="Shape">{[...NODE_LABELS, otherLabel].map(label => <KindShape key={label} label={label} />)}</Row>
          <Row name="Icon">{[...NODE_LABELS, otherLabel].map(label => <KindShape key={label} label={label} icon />)}</Row>
          <Row name="Faded">{[...NODE_LABELS, otherLabel].map(label => <KindShape key={label} label={label} faded />)}</Row>
        </Section>

        <Section id="document-types" title="Document types">
          <Row name="Type">{[...DOCUMENT_TYPE_KEYS, "vehicle_registration"].map(type => <TypeDot key={type} type={type} />)}</Row>
        </Section>

        <section aria-labelledby="buttons-title" className="space-y-4">
          <h2 id="buttons-title" className="text-base font-semibold">Buttons</h2>
          <div className="grid gap-4 md:grid-cols-2">
            {variants.map(variant => <div key={variant} className="flex flex-wrap items-center gap-2">
              <Button variant={variant}>{variant === "default" ? "Primary" : variant[0].toUpperCase() + variant.slice(1)}</Button>
              <Button variant={variant} disabled>Disabled</Button>
              <Button variant={variant} loading>Loading</Button>
            </div>)}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {sizes.map(size => <Button key={size} size={size} loading={loading}><FileText />Size {size === "default" ? "36" : { xs: "28", sm: "32", lg: "44" }[size]}</Button>)}
            {(["icon-xs", "icon-sm", "icon", "icon-lg"] as const).map(size => <Button key={size} size={size} variant="outline" aria-label={`Add document (${size})`}><Plus /></Button>)}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button data-testid="loading-button" loading={loading}><FileText />Save document</Button>
            <Button variant="ghost" onClick={() => setLoading(value => !value)} aria-pressed={loading}>Toggle loading</Button>
            <Button asChild variant="link"><a href="#fields-title">Go to fields</a></Button>
          </div>
        </section>

        <section aria-labelledby="fields-title" className="space-y-4">
          <h2 id="fields-title" className="text-base font-semibold">Fields and selection</h2>
          <div className="grid gap-4 md:grid-cols-3">
            <div className="space-y-2"><label htmlFor="title" className="text-xs">Document title</label><Input id="title" placeholder="Enter a title" /></div>
            <div className="space-y-2"><label htmlFor="invalid-title" className="text-xs">Required title</label><Input id="invalid-title" aria-invalid="true" aria-describedby="title-error" /><p id="title-error" className="text-xs text-error">Enter a document title.</p></div>
            <div className="space-y-2"><label htmlFor="disabled-title" className="text-xs">Imported title</label><Input id="disabled-title" defaultValue="Annual statement" disabled /></div>
            <div className="space-y-2"><label htmlFor="note" className="text-xs">Review note</label><Textarea id="note" placeholder="Add a note" /></div>
            <div className="space-y-2"><label htmlFor="invalid-note" className="text-xs">Required note</label><Textarea id="invalid-note" aria-invalid="true" aria-describedby="note-error" /><p id="note-error" className="text-xs text-error">Describe what you checked.</p></div>
            <div className="space-y-2"><label htmlFor="disabled-note" className="text-xs">Saved note</label><Textarea id="disabled-note" defaultValue="Reviewed against the source." disabled /></div>
          </div>
          <div className="flex flex-wrap gap-4">
            <label className="flex min-h-11 md:min-h-6 items-center gap-3 text-sm"><Checkbox />Unchecked</label>
            <label className="flex min-h-11 md:min-h-6 items-center gap-3 text-sm"><Checkbox defaultChecked />Checked</label>
            <label className="flex min-h-11 md:min-h-6 items-center gap-3 text-sm"><Checkbox checked="indeterminate" />Mixed</label>
            <label className="flex min-h-11 md:min-h-6 items-center gap-3 text-sm"><Checkbox disabled />Disabled</label>
            <label className="flex min-h-11 md:min-h-6 items-center gap-3 text-sm"><Checkbox disabled checked />Disabled checked</label>
          </div>
        </section>

        <section aria-labelledby="surfaces-title" className="space-y-4">
          <h2 id="surfaces-title" className="text-base font-semibold">Cards and badges</h2>
          <div className="grid gap-4 md:grid-cols-2">
            <Card><CardHeader><CardTitle>Document review</CardTitle><CardDescription>Check the source before accepting an extraction.</CardDescription></CardHeader><CardContent><p className="doc-title">Annual statement</p><p className="text-sm text-fg-muted">A synthetic document for this gallery.</p></CardContent><CardFooter><Button variant="secondary">Review document</Button></CardFooter></Card>
            <div className="space-y-4"><div className="flex flex-wrap gap-2">{([...variants, "dot", "status"] as const).map(variant => <Badge key={variant} variant={variant} className={variant === "dot" ? "before:bg-doc-work" : undefined}>{variant === "status" ? <Check aria-hidden="true" /> : null}{variant}</Badge>)}</div><div className="grid grid-cols-2 gap-2 text-xs">{["bg", "surface", "raised", "overlay"].map(surface => <div key={surface} className="rounded-lg border p-4" style={{ background: `var(--${surface})` }}>{surface}</div>)}</div></div>
          </div>
        </section>

        <section aria-labelledby="table-title" className="space-y-4">
          <h2 id="table-title" className="text-base font-semibold">Table and pager</h2>
          <div className="overflow-hidden rounded-lg border">
            <Table><TableCaption>Example documents. Select a row or change the title order.</TableCaption><TableHeader><TableRow><TableHead aria-sort={descending ? "descending" : "ascending"}><Button variant="ghost" size="sm" onClick={() => setDescending(value => !value)}>Title{descending ? <ArrowDown /> : <ArrowUp />}</Button></TableHead><TableHead>Pages</TableHead></TableRow></TableHeader><TableBody>
              {(descending ? [...documents].reverse() : documents).map((title, index) => <TableRow key={title} tabIndex={0} aria-selected={selected === title} data-state={selected === title ? "selected" : undefined} onClick={() => setSelected(value => value === title ? null : title)} onKeyDown={event => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); setSelected(value => value === title ? null : title); } }}><TableCell><span className="flex items-center gap-2"><FileText className="size-4" aria-hidden="true" />{title}</span></TableCell><TableCell>{index + 2}</TableCell></TableRow>)}
              <TableRow aria-label="Loading document"><TableCell><Skeleton className="h-4 w-40" /></TableCell><TableCell><Skeleton className="h-4 w-6" /></TableCell></TableRow>
            </TableBody></Table>
            <Pager page={page} pageCount={3} pageSize={10} total={23} onPage={setPage} />
          </div>
          <Pager page={0} pageCount={1} pageSize={10} total={0} onPage={() => {}} disabled noun="document" />
        </section>

        <section aria-labelledby="feedback-title" className="space-y-4">
          <h2 id="feedback-title" className="text-base font-semibold">Progress, skeleton and scroll area</h2>
          <div className="grid gap-6 md:grid-cols-2">
            <div className="space-y-4">{[0, 50, 100].map(value => <div key={value} className="space-y-2"><p className="text-xs tabular-nums">Import progress: {value}%</p><Progress value={value} aria-label={`Import progress ${value}%`} /></div>)}<Progress indeterminate aria-label="Waiting for import" /><Skeleton className="h-5 w-2/3" /><Skeleton className="h-4 w-full" /></div>
            <ScrollArea className="h-40 rounded-lg border p-4"><div className="space-y-3">{Array.from({ length: 12 }, (_, i) => <p className="text-sm" key={i}>Example archive document {i + 1}</p>)}</div></ScrollArea>
          </div>
        </section>

        <section aria-labelledby="overlays-title" className="space-y-4">
          <h2 id="overlays-title" className="text-base font-semibold">Dialogs, sheet, tooltip and toast</h2>
          <div className="flex flex-wrap gap-2">
            <AlertDialog><AlertDialogTrigger asChild><Button variant="secondary">Open alert dialog</Button></AlertDialogTrigger><AlertDialogContent><AlertDialogHeader><AlertDialogTitle>Merge Cedar Laboratory into Cedar Lab?</AlertDialogTitle><AlertDialogDescription>Cedar Lab keeps its name and takes over the relationships of Cedar Laboratory. You can undo the merge later.</AlertDialogDescription></AlertDialogHeader><AlertDialogFooter><AlertDialogCancel>Cancel</AlertDialogCancel><AlertDialogAction>Merge</AlertDialogAction></AlertDialogFooter></AlertDialogContent></AlertDialog>
            <Dialog><DialogTrigger asChild><Button variant="secondary">Open dialog</Button></DialogTrigger><DialogContent><DialogHeader><DialogTitle>Review document</DialogTitle><DialogDescription>Compare the extracted title with the source document.</DialogDescription></DialogHeader><p className="doc-title">Annual statement</p><DialogFooter><DialogClose asChild><Button>Done</Button></DialogClose></DialogFooter></DialogContent></Dialog>
            <Sheet><SheetTrigger asChild><Button variant="secondary">Open sheet</Button></SheetTrigger><SheetContent><SheetHeader><SheetTitle>Source document</SheetTitle><SheetDescription>A source stays within reach while you review.</SheetDescription></SheetHeader><p className="doc-title px-4">Annual statement</p><SheetFooter><SheetClose asChild><Button>Done</Button></SheetClose></SheetFooter></SheetContent></Sheet>
            <Tooltip><TooltipTrigger asChild><Button variant="outline"><Plus />Add to archive</Button></TooltipTrigger><TooltipContent>Add a document for review</TooltipContent></Tooltip>
          </div>
          {/* Inline and untimed here; pages use Toaster, which times out and announces. */}
          <Toast toast={{ id: 0, text: "Merged Cedar Laboratory into Cedar Lab.", action: { label: "Undo", onClick: () => {} } }} onDismiss={() => {}} className="animate-none" />
        </section>
      </div>
    </div>
  );
}
