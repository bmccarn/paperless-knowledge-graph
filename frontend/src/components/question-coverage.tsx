export interface QuestionCoverageReceipt {
  status: string;
  complete: boolean;
  planning_status?: string;
  conservation_status?: string;
  acquisition_complete?: boolean;
  omitted_requested_aspects?: boolean | null;
  requirements?: Array<{
    requirement_id: string;
    aspect: string;
    status: string;
  }>;
}

export function QuestionCoverage({ receipt }: { receipt?: QuestionCoverageReceipt }) {
  if (!receipt) return null;
  const rows = receipt.requirements || [];
  const complete = receipt.status === "complete" && receipt.complete === true;
  const unavailable = receipt.status === "unavailable";
  const answered = rows.filter(row => row.status === "answered").length;
  const labels: Record<string, string> = {
    answered: "Answered", partial: "Partly answered", unresolved: "Not answered", unavailable: "Not assessed",
  };
  return (
    <section aria-label="Question coverage" className="rounded-lg border bg-card/70 px-3 py-2 text-xs space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="font-medium">Question coverage</p>
        <span className={complete ? "text-emerald-700 dark:text-emerald-400" : "text-amber-700 dark:text-amber-400"}>
          {unavailable ? "Coverage unavailable" : complete ? "Requested aspects answered" : "Partly answered"}
        </span>
      </div>
      <p className="text-muted-foreground">
        {unavailable ? "The displayed answer has no complete coverage assessment." :
          `${answered} of ${rows.length} requested aspects answered. This is separate from source support.`}
      </p>
      {rows.length > 0 && <ul className="space-y-1.5">
        {rows.map(row => <li key={row.requirement_id} className="flex flex-wrap items-start justify-between gap-x-3 gap-y-0.5">
          <span className="min-w-0 flex-1 break-words">{row.aspect}</span>
          <span className="text-muted-foreground">{labels[row.status] || "Not assessed"}</span>
        </li>)}
      </ul>}
      {receipt.acquisition_complete === false &&
        <p className="text-amber-700 dark:text-amber-400">Some source searches or document reads could not finish. Coverage remains partial.</p>}
      {receipt.conservation_status === "partial" &&
        <p className="text-amber-700 dark:text-amber-400">Some source information may be missing from this answer.</p>}
      {receipt.conservation_status === "unavailable" &&
        <p className="text-amber-700 dark:text-amber-400">The check for missing source information could not finish.</p>}
      {(receipt.omitted_requested_aspects || (receipt.planning_status && receipt.planning_status !== "complete")) &&
        <p className="text-amber-700 dark:text-amber-400">Some parts of your question may be missing from this assessment.</p>}
    </section>
  );
}
