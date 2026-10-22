import "react";
import type { SAppNavAttributes } from "@shopify/app-bridge-types";

declare module "react" {
  namespace JSX {
    interface IntrinsicElements {
      "s-app-nav": SAppNavAttributes;
    }
  }
}
