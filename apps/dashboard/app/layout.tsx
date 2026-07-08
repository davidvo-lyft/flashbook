/** Root layout: dark shell, top nav, honest footer. */

import type { Metadata } from "next";
import type { ReactNode } from "react";
import Nav from "@/components/Nav";
import "./globals.css";

export const metadata: Metadata = {
  title: "flashbook dashboard",
  description:
    "Static evidence dashboard for the flashbook market-data capture/replay pipeline: corpus stats, replayed order books, ingest health, benchmarks.",
};

/** App shell shared by every page. */
export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Nav />
        <main className="page">{children}</main>
        <footer className="footer">
          static export · all figures derive from capture artifacts, not live feeds
        </footer>
      </body>
    </html>
  );
}
