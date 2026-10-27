import type { ActionFunctionArgs } from "react-router";
import { receiveSourceCsv } from "../services/source-intake.server";

export const action = ({ request }: ActionFunctionArgs) =>
  receiveSourceCsv(request);
export const loader = () =>
  Response.json(
    { error: "Use POST." },
    { status: 405, headers: { Allow: "POST" } },
  );
