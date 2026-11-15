import type { ReactNode } from "react";
import { EvidenceLevel, StatusLabel } from "@/components/status-label";
import { KindShape, TypeDot } from "@/components/kind-shape";
import type { AnswerState, ClaimResult } from "@/lib/answer-state";
import { DOCUMENT_TYPE_KEYS, NODE_LABELS } from "@/lib/categories";

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

/** Not linked from navigation. #113's component gallery takes these sections once it is merged. */
export default function StateGalleryPage() {
  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-6xl space-y-8 p-4 md:px-12 md:py-10">
        <header className="space-y-2">
          <h1 className="page-title">States and categories</h1>
          <p className="copy-14 text-fg-muted">How the archive marks answer checks, evidence, entity kinds and document types.</p>
        </header>

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
      </div>
    </div>
  );
}
