import { redirect } from "react-router";

// Keep existing bookmarks working after removing the template demonstration.
export const loader = () => redirect("/app");
