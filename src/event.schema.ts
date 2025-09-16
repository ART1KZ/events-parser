import { z } from "zod";

export const EventSchema = z.object({
  source: z.string().min(1),
  sourceId: z.string().min(1),
  name: z.string().min(1),
  url: z.string().url(),
  startDate: z.string().min(1), // ISO 8601
  endDate: z.string().optional(),
  hall: z.string().optional(),
  format: z.string().optional(),
  language: z.string().optional(),
  priceFrom: z.number().optional(),
});

export type EventDTO = z.infer<typeof EventSchema>;
