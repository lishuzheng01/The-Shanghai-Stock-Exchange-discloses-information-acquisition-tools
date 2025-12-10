import csv
import time
import requests

# ================== 配置区 =====================

# 股票代码来源：从 CSV 文件读取（第一列字段名：code）
# 注意：CSV 及时更新，每次运行前请检查是否有新股票上市、或者有股票退市
CODES_CSV = "mainboard_codes.csv"
"""
打开上交所官网：股票与存托凭证 → 股票列表（就是你看到 主板A股 那个页面）
板块选择：主板A股
页面一般会有“下载 / 导出”按钮（导出 Excel 或 CSV）；
导出后用 Excel 打开，把只需要的列保留下来，比如：
...
code
600000
600004
600006
...
另存为 UTF-8 编码的 CSV 文件，命名为：mainboard_codes.csv，放在脚本同一目录。
之后脚本会自动读取这个 CSV，把里面所有 code 当作“全部主板股票”。
"""
# 年份范围（闭区间）
START_YEAR = 2022
END_YEAR = 2023

# 上交所定期报告查询接口
URL_QUERY_COMPANY = "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
# PDF 静态文件基础地址
URL_PDF_BASE = "https://static.sse.com.cn"

HEADERS = {
    "Referer": "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
    "User-Agent": "Mozilla/5.0",
}

MAX_RETRIES = 3


# ================== 工具函数 =====================

def load_codes_from_csv(path):
    """从 mainboard_codes.csv 中读取全部主板股票代码"""
    codes = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # 默认列名叫 "code"，你也可以改成别的，下面同时适配常见几种
        for row in reader:
            code = (row.get("code")
                    or row.get("证券代码")
                    or row.get("stock_code")
                    or "").strip()
            if code:
                codes.append(code)
    print(f"[INFO] 从 {path} 读取到 {len(codes)} 只股票代码")
    return codes


def request_with_retry(session, method, url, max_retries=MAX_RETRIES, **kwargs):
    """带重试的请求封装"""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, timeout=15, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"[WARN] 请求失败 {attempt}/{max_retries} 次: {e}")
            if attempt == max_retries:
                raise
            time.sleep(2 * attempt)


def fetch_reports_for_year(session, code, year):
    """获取某股票某年的定期报告列表（年报）"""
    begin_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    params = {
        "isPagination": "false",   # 不分页
        "productId": code,         # 股票代码
        "keyWord": "",
        "securityType": "0101",    # 主板 A 股：0101（如果你 CSV 里有科创板，就要再扩展）
        "reportType2": "DQBG",     # 定期报告
        "reportType": "YEARLY",    # YEARLY = 年报
        "beginDate": begin_date,
        "endDate": end_date,
    }

    resp = request_with_retry(session, "GET", URL_QUERY_COMPANY,
                              headers=HEADERS, params=params)
    data = resp.json()
    results = data.get("result", [])
    print(f"[INFO] {code} {year} 年获取到 {len(results)} 条记录")
    return results


# ================== 主逻辑：遍历全部主板股票 =====================

def main():
    session = requests.Session()

    # 1. 读入全部主板股票代码
    codes = load_codes_from_csv(CODES_CSV)

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n===== 处理年份：{year} =====")

        summary_rows = []   # 该年份所有股票的所有年报记录
        seen_urls = set()   # 按 URL 去重

        for idx, code in enumerate(codes, start=1):
            print(f"\n[CODE] ({idx}/{len(codes)}) 处理股票：{code}")
            try:
                reports = fetch_reports_for_year(session, code, year)
            except Exception as e:
                print(f"[ERROR] 获取 {code} {year} 年报告失败：{e}")
                continue

            for item in reports:
                title = (item.get("TITLE") or "").strip()
                date = (item.get("SSEDATE") or "").strip()
                relative_url = (item.get("URL") or "").strip()

                if not relative_url:
                    continue

                # 补全为完整链接
                if not relative_url.startswith("http"):
                    pdf_url = URL_PDF_BASE + relative_url
                else:
                    pdf_url = relative_url

                # 无论哪个股票来的，只要 URL 一样，就视作同一份公告 → 去重
                if pdf_url in seen_urls:
                    # 若你希望同一公告在多个 code 上各保留一行，可把这个去重逻辑改成按 (code, url) 去重
                    print(f"[DUP] 已存在，跳过：{pdf_url}")
                    continue
                seen_urls.add(pdf_url)

                print(f"[LINK] {code} | {date} | {title}")
                summary_rows.append({
                    "code": code,
                    "title": title,
                    "date": date,
                    "url": pdf_url,
                })

                # 稍微礼貌一点
                time.sleep(0.1)

        # 2. 写出该年份的汇总 CSV：标题，日期，链接（加上代码）
        if summary_rows:
            csv_name = f"summary_mainboard_links_{year}.csv"
            with open(csv_name, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f, fieldnames=["code", "title", "date", "url"]
                )
                writer.writeheader()
                writer.writerows(summary_rows)
            print(f"\n[OK] {year} 年主板股票年报链接汇总已写入：{csv_name}")
        else:
            print(f"\n[WARN] {year} 年没有任何有效记录。")

    print("\n全部任务完成！")


if __name__ == "__main__":
    main()
