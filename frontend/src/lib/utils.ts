import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function errMsg(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback
}
