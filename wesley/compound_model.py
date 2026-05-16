"""
Magic Lab — 複利效應計算引擎
Wesley (Chief Everything Officer) 建立
版本: v1.0  |  2026-05-16
公式: 小選擇 × 持續性 × 時間 = 財富自由
"""

from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class CompoundConfig:
    """複利計算輸入假設"""
    principal: float = 300_000       # 初始本金（元）
    monthly_invest: float = 5_000    # 每月定期定額（元）
    annual_rate: float = 0.08        # 年化報酬率
    inflation_rate: float = 0.02     # 通膨率
    years: int = 20                  # 投資年數


@dataclass
class YearlyResult:
    year: int
    opening_assets: float
    yearly_invest: float
    compound_gain: float
    closing_assets: float
    real_purchasing_power: float


@dataclass
class SkillScore:
    """Magic Lab 四維選股評分"""
    ticker: str
    name: str
    terry_score: float    # 創意面（品牌/故事）
    miles_score: float    # 執行面（基本面）
    wayne_score: float    # 創新面（成長/趨勢）
    wesley_score: float   # 風險面（穩定性）
    weights: Dict[str, float] = field(default_factory=lambda: {
        "terry": 0.25, "miles": 0.30, "wayne": 0.30, "wesley": 0.15
    })

    @property
    def weighted_score(self) -> float:
        return (self.terry_score  * self.weights["terry"] +
                self.miles_score  * self.weights["miles"] +
                self.wayne_score  * self.weights["wayne"] +
                self.wesley_score * self.weights["wesley"])

    @property
    def recommendation(self) -> str:
        s = self.weighted_score
        if s >= 8:   return "⭐ 強力買進"
        if s >= 6.5: return "✅ 考慮買進"
        if s >= 5:   return "⚠️  觀察"
        return "❌ 暫不考慮"


class CompoundEngine:
    """複利計算引擎"""

    def __init__(self, config: CompoundConfig):
        self.cfg = config

    def run(self) -> List[YearlyResult]:
        results = []
        assets = self.cfg.principal
        yearly_invest = self.cfg.monthly_invest * 12

        for yr in range(1, self.cfg.years + 1):
            gain = (assets + yearly_invest / 2) * self.cfg.annual_rate
            closing = assets + yearly_invest + gain
            real_power = closing / (1 + self.cfg.inflation_rate) ** yr
            results.append(YearlyResult(
                year=yr,
                opening_assets=round(assets),
                yearly_invest=round(yearly_invest),
                compound_gain=round(gain),
                closing_assets=round(closing),
                real_purchasing_power=round(real_power),
            ))
            assets = closing
        return results

    def summary(self, results: List[YearlyResult]) -> Dict:
        final = results[-1]
        total_invested = (self.cfg.principal +
                          self.cfg.monthly_invest * 12 * self.cfg.years)
        pure_gain = final.closing_assets - total_invested
        return {
            "總投入本金":   round(total_invested),
            "期末資產":     round(final.closing_assets),
            "純複利收益":   round(pure_gain),
            "資產成長倍數": round(final.closing_assets / self.cfg.principal, 2),
            "實質購買力":   round(final.real_purchasing_power),
        }


class StockScreener:
    """Magic Lab 四維選股篩選器"""

    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or {
            "terry": 0.25, "miles": 0.30, "wayne": 0.30, "wesley": 0.15
        }

    def screen(self, stocks: List[SkillScore]) -> List[SkillScore]:
        for s in stocks:
            s.weights = self.weights
        return sorted(stocks, key=lambda x: x.weighted_score, reverse=True)

    def report(self, stocks: List[SkillScore]) -> None:
        print("\n" + "="*60)
        print("Magic Lab 四維選股評分報告")
        print("="*60)
        fmt = "{:<8} {:<10} {:>6} {:>6} {:>6} {:>6} {:>8}  {}"
        print(fmt.format("代碼","名稱","Terry","Miles","Wayne","Wesley","加權分","建議"))
        print("-"*60)
        for s in stocks:
            print(fmt.format(
                s.ticker, s.name,
                f"{s.terry_score:.1f}", f"{s.miles_score:.1f}",
                f"{s.wayne_score:.1f}", f"{s.wesley_score:.1f}",
                f"{s.weighted_score:.2f}", s.recommendation
            ))
        print("="*60)


# ── 範例執行 ────────────────────────────────────────────
if __name__ == "__main__":

    # 1. 複利計算
    print("\n[Magic Lab 複利計算引擎]")
    engine = CompoundEngine(CompoundConfig(
        principal=300_000, monthly_invest=5_000,
        annual_rate=0.08, years=20
    ))
    results = engine.run()
    summary = engine.summary(results)
    print(f"  初始本金: {300_000:,} 元")
    print(f"  每月定投: {5_000:,} 元")
    print(f"  年化報酬: 8%  |  投資年數: 20年")
    print()
    for k, v in summary.items():
        print(f"  {k}: {v:,}")

    # 里程碑
    print("\n[複利里程碑]")
    for yr in [5, 10, 15, 20]:
        r = results[yr-1]
        print(f"  {yr:2d}年: {r.closing_assets:>12,} 元  （實質購買力 {r.real_purchasing_power:>12,} 元）")

    # 2. 四維選股
    print()
    screener = StockScreener()
    portfolio = [
        SkillScore("2330","台積電",    9, 8, 9, 7),
        SkillScore("2317","鴻海",      7, 8, 7, 7),
        SkillScore("2454","聯發科",    8, 7, 9, 6),
        SkillScore("0050","元大台灣50", 6, 9, 6, 9),
        SkillScore("00878","國泰永續高股息", 5, 8, 5, 9),
    ]
    ranked = screener.screen(portfolio)
    screener.report(ranked)
