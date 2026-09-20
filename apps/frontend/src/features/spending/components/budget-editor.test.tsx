import { render, screen } from "@/test/render";
import userEvent from "@testing-library/user-event";
import { TooltipProvider } from "@wealthfolio/ui";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type { BudgetSnapshot } from "../types/budget";
import { BudgetEditor } from "./budget-editor";

const { useBudget, upsertRollover } = vi.hoisted(() => ({
  useBudget: vi.fn(),
  upsertRollover: vi.fn(),
}));

vi.mock("../hooks/use-budget", () => ({
  useBudget,
  useBudgetMutations: () => ({
    upsertRollover: { mutate: upsertRollover },
    removeGroup: { isPending: false },
  }),
}));

describe("BudgetEditor setup rollover", () => {
  it.each(["category", "group"] as const)(
    "uses the saved %s setting when computed monthly rollover is disabled",
    async (targetType) => {
      upsertRollover.mockClear();
      const user = userEvent.setup();
      const group = {
        id: "essentials",
        name: "Essentials",
        key: "essentials",
        color: null,
        icon: null,
        sortOrder: 0,
        isSystem: false,
        createdAt: "2026-01-01",
        updatedAt: "2026-01-01",
      };
      const budget: BudgetSnapshot = {
        state: {
          groups: [group],
          groupAssignments: [],
          targets: [],
          rolloverSettings: [
            {
              id: "rollover",
              targetType,
              taxonomyId: targetType === "category" ? "spending_categories" : null,
              categoryId: targetType === "category" ? "groceries" : null,
              groupId: targetType === "group" ? group.id : null,
              enabled: true,
              startMonth: "2026-01",
              startingBalance: "25",
              createdAt: "2026-01-01",
              updatedAt: "2026-01-01",
            },
          ],
        },
        computed: {
          currency: "USD",
          periodKey: "default",
          fxAsOf: null,
          groupRows: [
            {
              group,
              categoryTargetTotal: 0,
              buffer: 0,
              plannedTotal: 0,
              actual: 0,
              rolloverIn: 0,
              rolloverOut: 0,
              remaining: 0,
              overspent: false,
              rolloverEnabled: false,
              categories: [
                {
                  taxonomyId: "spending_categories",
                  categoryId: "groceries",
                  groupId: group.id,
                  parentId: null,
                  name: "Groceries",
                  color: null,
                  icon: null,
                  target: 0,
                  actual: 0,
                  rolloverIn: 0,
                  rolloverOut: 0,
                  remaining: 0,
                  overspent: false,
                  hasDefaultTarget: false,
                  hasMonthOverride: false,
                  rolloverEnabled: false,
                },
              ],
            },
          ],
          ungroupedRows: [],
          incomeRows: [],
          totals: {
            spendingPlanned: 0,
            spendingActual: 0,
            spendingRemaining: 0,
            incomePlanned: 0,
            incomeActual: 0,
            groupBuffer: 0,
            rolloverIn: 0,
            rolloverOut: 0,
            overspentCount: 0,
          },
        },
      };
      useBudget.mockReturnValue({ data: budget, isLoading: false, error: null });
      render(
        <MemoryRouter>
          <TooltipProvider>
            <BudgetEditor mode="setup" periodKey="default" />
          </TooltipProvider>
        </MemoryRouter>,
      );
      await user.click(screen.getByRole("button", { name: "Expand group" }));
      await user.click(screen.getByRole("button", { name: "Row options" }));

      if (targetType === "category") {
        const toggle = screen.getByRole("menuitem", { name: /^Rollover\s*On$/ });
        expect(toggle).not.toHaveAttribute("aria-disabled", "true");
        await user.click(toggle);
        expect(upsertRollover).toHaveBeenCalledOnce();
        expect(upsertRollover).toHaveBeenCalledWith(
          expect.objectContaining({
            targetType: "category",
            taxonomyId: "spending_categories",
            categoryId: "groceries",
            enabled: false,
            startingBalance: "25",
          }),
        );
      } else {
        const toggle = screen.getByRole("menuitem", { name: /^Rollover\s*Group-wide$/ });
        expect(toggle).toHaveAttribute("aria-disabled", "true");
        await user.click(toggle);
        expect(upsertRollover).not.toHaveBeenCalled();
      }
    },
  );
});
