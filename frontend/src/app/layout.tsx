import type { Metadata } from "next";
import { Geist, Geist_Mono, Source_Serif_4 } from "next/font/google";
import { GeistSerifRegular } from "geist/font/serif/regular";
import "./globals.css";
import { ThemeProvider } from "next-themes";
import { SidebarNav } from "@/components/sidebar-nav";
import { TooltipProvider } from "@/components/ui/tooltip";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });
const sourceSerif = Source_Serif_4({
  variable: "--font-source-serif",
  subsets: ["latin"],
  style: ["normal", "italic"],
  axes: ["opsz"],
});

export const metadata: Metadata = {
  title: "Knowledge Graph",
  description: "Paperless Knowledge Graph Explorer",
  icons: {
    icon: "/favicon.ico",
    apple: "/apple-touch-icon.png",
  },
  manifest: "/site.webmanifest",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
      </head>
      <body className={`${geistSans.variable} ${geistMono.variable} ${GeistSerifRegular.variable} ${sourceSerif.variable} antialiased`}>
        <ThemeProvider attribute="class" defaultTheme="dark" enableSystem disableTransitionOnChange>
          <TooltipProvider delayDuration={200}>
            <a href="#main-content" className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[100] focus:rounded-md focus:bg-background focus:px-4 focus:py-2">Skip to content</a>
            <div className="flex h-dvh min-h-dvh overflow-hidden">
              <SidebarNav />
              <main id="main-content" tabIndex={-1} className="min-h-0 min-w-0 flex-1 overflow-hidden flex flex-col pt-14 md:pt-0 pb-[calc(var(--tabbar)+env(safe-area-inset-bottom))] md:pb-0 has-[[data-gallery-scroll]]:pb-0 has-[[data-gallery-scroll]]:pt-0">
                <div className="flex-1 overflow-hidden relative">{children}</div>
              </main>
            </div>
          </TooltipProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
