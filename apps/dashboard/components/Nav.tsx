"use client";

/** Top navigation bar with active-route highlighting. */

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "overview" },
  { href: "/book", label: "book" },
  { href: "/ingest", label: "ingest" },
  { href: "/bench", label: "bench" },
] as const;

/** Site header: brand + section links. */
export default function Nav() {
  const path = usePathname() ?? "/";
  return (
    <header className="nav">
      <span className="nav-brand">
        flashbook<span className="nav-brand-dim">/dashboard</span>
      </span>
      <nav>
        {LINKS.map((l) => {
          const active = l.href === "/" ? path === "/" : path.startsWith(l.href);
          return (
            <Link key={l.href} href={l.href} className={active ? "nav-link nav-active" : "nav-link"}>
              {l.label}
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
