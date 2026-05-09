import { STATUTES, type Statute } from "./statutes";

export type Matter = {
  id: string;
  name: string;
  caption: string;
  factors: string[];
  coverageGap: string;
  authoritiesCount: number;
  statutes: Statute[];
};

export const CURRENT_MATTER: Matter = {
  id: "reyes-western-logistics",
  name: "Reyes v. Western Logistics",
  caption: "Rear-end collision, I-880",
  factors: ["Following Too Closely", "Speeding", "Reckless Driving", "Distracted Driving"],
  coverageGap: "commercial trucking regulations",
  authoritiesCount: 17,
  statutes: STATUTES.slice(0, 6),
};
