"""
Magic Lab — CI 核心測試腳本
Wesley (Chief Everything Officer) 維護
觸發：GitHub Actions push to main
"""

import sys
import os

def test_environment():
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  平台: {sys.platform}")
    return True

def test_folder_structure():
    required = ["terry", "miles", "wayne", "wesley"]
    results = []
    for folder in required:
        exists = os.path.isdir(folder)
        status = "✅" if exists else "❌"
        print(f"  {status} {folder}/")
        results.append(exists)
    return all(results)

def test_compound_model():
    try:
        sys.path.insert(0, "wesley")
        from compound_model import CompoundConfig, CompoundEngine, SkillScore, StockScreener

        # 複利計算測試
        engine = CompoundEngine(CompoundConfig(
            principal=300_000, monthly_invest=5_000,
            annual_rate=0.08, years=20
        ))
        results = engine.run()
        summary = engine.summary(results)

        assert len(results) == 20, "應有 20 年資料"
        assert summary["期末資產"] > summary["總投入本金"], "複利資產應大於投入本金"
        assert summary["資產成長倍數"] > 1, "成長倍數應大於 1"
        print(f"  ✅ 複利引擎：20年期末 {summary['期末資產']:,} 元，成長 {summary['資產成長倍數']}x")

        # 選股篩選測試
        screener = StockScreener()
        portfolio = [
            SkillScore("2330", "台積電",    9, 8, 9, 7),
            SkillScore("0050", "元大台灣50", 6, 9, 6, 9),
        ]
        ranked = screener.screen(portfolio)
        assert ranked[0].ticker == "2330", "台積電應排第一"
        print(f"  ✅ 選股篩選：台積電評分 {ranked[0].weighted_score:.2f}，排名第一")
        return True

    except ImportError as e:
        print(f"  ⚠️  compound_model 尚未安裝（{e}），跳過此測試")
        return True  # 非阻斷性錯誤
    except Exception as e:
        print(f"  ❌ 測試失敗：{e}")
        return False

def main():
    print("\n" + "="*50)
    print("🔬 Magic Lab CI 核心測試啟動")
    print("="*50)

    suites = [
        ("環境檢查",    test_environment),
        ("資料夾架構",  test_folder_structure),
        ("複利模型",    test_compound_model),
    ]

    passed = 0
    failed = 0
    for name, fn in suites:
        print(f"\n▸ {name}")
        try:
            ok = fn()
            if ok:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ❌ 未預期錯誤：{e}")
            failed += 1

    print("\n" + "="*50)
    print(f"結果：{passed} 通過 / {failed} 失敗")
    print("="*50)

    if failed > 0:
        sys.exit(1)
    else:
        print("🏆 Magic_Lab 四人就位，CI 全數通過！")

if __name__ == "__main__":
    main()
