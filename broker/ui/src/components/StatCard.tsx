import { motion } from "framer-motion";

interface StatCardProps {
  label: string;
  value: number;
  /** Tone drives the accent dot + value color. */
  tone?: "neutral" | "accent" | "warn" | "danger" | "info";
  /** Short helper under the value. */
  hint?: string;
  index?: number;
}

const TONE: Record<NonNullable<StatCardProps["tone"]>, string> = {
  neutral: "text-text",
  accent: "text-accent",
  warn: "text-warn",
  danger: "text-danger",
  info: "text-info",
};

const DOT: Record<NonNullable<StatCardProps["tone"]>, string> = {
  neutral: "bg-muted",
  accent: "bg-accent",
  warn: "bg-warn",
  danger: "bg-danger",
  info: "bg-info",
};

export function StatCard({ label, value, tone = "neutral", hint, index = 0 }: StatCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.25, ease: "easeOut", delay: index * 0.03 }}
      className="surface relative overflow-hidden p-5"
    >
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-muted">{label}</span>
        <span className={`h-2 w-2 rounded-full ${DOT[tone]}`} aria-hidden />
      </div>
      {/* value: tabular nums keep the column from jittering as counts change */}
      <div className={`mt-3 font-mono text-3xl font-semibold tabular-nums ${TONE[tone]}`}>
        {value.toLocaleString()}
      </div>
      {hint && <div className="mt-1 text-xs text-subtle">{hint}</div>}
    </motion.div>
  );
}
