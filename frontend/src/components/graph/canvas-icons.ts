/** The lucide kind icons as canvas paths, from the installed icons' geometry, so they draw on the first frame. */
import * as BuildingComplex from "lucide-react/icons/building-complex";
import * as Calendar from "lucide-react/icons/calendar";
import * as CalendarClock from "lucide-react/icons/calendar-clock";
import * as CircleQuestionMark from "lucide-react/icons/circle-question-mark";
import * as Cpu from "lucide-react/icons/cpu";
import * as FilePenLine from "lucide-react/icons/file-pen-line";
import * as FileQuestionMark from "lucide-react/icons/file-question-mark";
import * as FileText from "lucide-react/icons/file-text";
import * as FlaskConical from "lucide-react/icons/flask-conical";
import * as House from "lucide-react/icons/house";
import * as MapPin from "lucide-react/icons/map-pin";
import * as Package from "lucide-react/icons/package";
import * as Receipt from "lucide-react/icons/receipt";
import * as ShieldCheck from "lucide-react/icons/shield-check";
import * as Stethoscope from "lucide-react/icons/stethoscope";
import * as UserRound from "lucide-react/icons/user-round";

type IconModule = { __iconData?: { node: [string, Record<string, string>][] } };

// Same icons as KIND_ICONS in kind-shape.tsx; aliases there (Building2, FileSignature, FileQuestion, CircleHelp) name these files.
const MODULES: Record<string, object> = {
  Person: UserRound, Organization: BuildingComplex, Location: MapPin, Address: House, FinancialItem: Receipt,
  Contract: FilePenLine, InsurancePolicy: ShieldCheck, Product: Package, System: Cpu, Condition: Stethoscope,
  MedicalResult: FlaskConical, Event: Calendar, DateEvent: CalendarClock, Document: FileText, DocumentRef: FileQuestionMark,
};

/** One lucide element as SVG path data in its 24 x 24 box. */
function elementPath(tag: string, a: Record<string, string>): string {
  const n = (key: string) => Number(a[key] ?? 0);
  if (tag === "path") return a.d;
  if (tag === "circle") return `M${n("cx") - n("r")},${n("cy")}a${n("r")},${n("r")} 0 1,0 ${2 * n("r")},0a${n("r")},${n("r")} 0 1,0 ${-2 * n("r")},0`;
  if (tag === "polyline") return `M${a.points.trim().split(/\s+/).reduce((out, value, index) => out + (index % 2 ? "," : index ? "L" : "") + value, "")}`;
  if (tag === "rect") {
    const [x, y, w, h, r] = [n("x"), n("y"), n("width"), n("height"), n("rx")];
    return `M${x + r},${y}H${x + w - r}a${r},${r} 0 0 1 ${r},${r}V${y + h - r}a${r},${r} 0 0 1 ${-r},${r}H${x + r}a${r},${r} 0 0 1 ${-r},${-r}V${y + r}a${r},${r} 0 0 1 ${r},${-r}Z`;
  }
  return "";
}

const cache = new Map<string, Path2D | null>();

/** The kind icon of a node label as SVG path data, one entry per element, in a 24 x 24 box; unknown labels get CircleHelp. */
export function iconData(label: string): string[] | null {
  const data = ((MODULES[label] ?? CircleQuestionMark) as IconModule).__iconData;
  return data ? data.node.map(([tag, attributes]) => elementPath(tag, attributes)) : null;
}

/** The kind icon of a node label as one canvas path. */
export function iconPath(label: string): Path2D | null {
  if (!cache.has(label)) {
    const data = iconData(label);
    // One subpath per element: a relative move at the start of an element must not follow the previous one.
    const path = data ? new Path2D() : null;
    for (const d of data ?? []) path!.addPath(new Path2D(d));
    cache.set(label, path);
  }
  return cache.get(label)!;
}
