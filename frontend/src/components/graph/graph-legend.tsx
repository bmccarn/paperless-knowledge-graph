const NODE_PALETTE: Record<string, { color: string; label: string }> = {
  Person:           { color: '#818cf8', label: 'Person' },
  Organization:     { color: '#34d399', label: 'Organization' },
  Document:         { color: '#64748b', label: 'Document' },
  DocumentRef:      { color: '#94a3b8', label: 'Doc Reference' },
  Location:         { color: '#2dd4bf', label: 'Location' },
  Address:          { color: '#22d3ee', label: 'Address' },
  MedicalResult:    { color: '#fb7185', label: 'Medical' },
  Medical_Result:   { color: '#fb7185', label: 'Medical' },
  Condition:        { color: '#f43f5e', label: 'Condition' },
  FinancialItem:    { color: '#fbbf24', label: 'Financial' },
  Financial_Item:   { color: '#fbbf24', label: 'Financial' },
  Contract:         { color: '#d97706', label: 'Contract' },
  InsurancePolicy:  { color: '#a3e635', label: 'Insurance' },
  System:           { color: '#60a5fa', label: 'System' },
  Product:          { color: '#c084fc', label: 'Product' },
  Event:            { color: '#f472b6', label: 'Event' },
  DateEvent:        { color: '#fb923c', label: 'Date' },
};

const DEFAULT_NODE_COLOR = '#c084fc';

export function getNodeColor(label: string): string {
  return NODE_PALETTE[label]?.color || DEFAULT_NODE_COLOR;
}
