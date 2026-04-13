# minimal_chatbot
only ses faculty

--------------Web crawling and DB creation------------------------------------------------------------------------------------

Start venv and initiate the schema
1. .venv\Scripts\activate
2. cd DB_Service
3. npm install
4. npm run schema

Run the Spiders to create json data
1. cd scrapy_crawler
2. scrapy crawl curricula_links_level2_ects -O spider_outputs\program_links_with_ects.json
3. scrapy crawl faculty_links -O spider_outputs\faculties.json
4. scrapy crawl unifr_ses_studyplans -O spider_outputs\faculty_programs\ses.json
5. scrapy crawl reglementation -O spider_outputs\reglementation_docs.json

Merge crawled docs
1. python DB_service\src\import\normalize_faculty_jsons.py ^  --input-dir scrapy_crawler\spider_outputs\faculty_programs ^  --out scrapy_crawler\spider_outputs\faculty_programs_normalized.json
 
2. python DB_service\src\import\merge_studyplans.py ^  --base scrapy_crawler\spider_outputs\program_links_with_ects.json ^  --inputs scrapy_crawler\spider_outputs\faculty_programs_normalized.json ^  --out scrapy_crawler\spider_outputs\program_links_with_ects_and_docs.json

3. python DB_service\src\import\unmatched_patch.py ^  --in "scrapy_crawler\spider_outputs\program_links_with_ects_and_docs.json" ^  --out "scrapy_crawler\spider_outputs\program_links_with_ects_and_docs_enriched.json"

Download and parse
1. npm i axios tough-cookie axios-cookiejar-support
2. npx ts-node DB_service\src\import\01_download_program_docs_v2.ts --input scrapy_crawler\spider_outputs\program_links_with_ects_and_docs_enriched.json --out scrapy_crawler/outputs
3. npx ts-node DB_service/src/import/parse_docs_full.ts --root scrapy_crawler/outputs
4. npx ts-node DB_service/src/import/reglementation_download_docs.ts   --input scrapy_crawler/spider_outputs/reglementation_docs.json \  --out scrapy_crawler/outputs/reglementation_docs
5. npx ts-node DB_service/src/import/parse_reglementation_docs_full.ts --root scrapy_crawler/outputs/reglementation_docs

Do the imports 
1. npx ts-node DB_service/src/import/run_faculty_import.ts
2. npx ts-node DB_service/src/import/run_courses_import.ts
3. npx ts-node DB_service/src/import/update_professors_from_people.ts
4. npx ts-node DB_service/src/import/program_name_imports.ts
5. npx ts-node DB_service/src/import/new_program_import.ts
6. npx ts-node DB_service/src/import/import_consist_of.ts
7. npx ts-node DB_service/src/import/run_reglementation_import.ts --root scrapy_crawler/outputs/reglementation_docs

--------------BACKEND API------------------------------------------------------------------------------------
Start the server
- cd backend_api
- npm run dev
- (see swagger at http://localhost:3002/docs)

start chatbot from folder
- ollama serve
- cd chatbot
- uvicorn app.main:app --reload --port 8001

start frontend
- cd frontend 
- npm start -- --port 4201

start admirer
- docker run -d -p 8080:8080 --name adminer adminer

netstat -ano | findstr :11434 (or port too free)
taskkill /PID resultabove /F

npx ts-node DB_service/src/import/parse_docs_full_llamaparse.ts --root scrapy_crawler/outputs --parser llamaparse --llamaparse-helper DB_service/src/import/parse_with_llamaparse.py

python -m app.build_faiss_llamaparse --target studyplans --parser llamaparse --force


npx ts-node DB_service/src/import/parse_docs_full_docling.ts --root scrapy_crawler/outputs --docling-helper DB_service/src/import/parse_with_docling.py

npx ts-node DB_service/src/import/parse_docs_full_docling.ts --root scrapy_crawler/outputs --docling-helper DB_service/src/import/parse_with_docling.py

python -m app.build_faiss_docling --target studyplans --parser docling --force
python -m app.build_faiss_docling --target regulations --parser docling --force
