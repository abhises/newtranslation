import TranslationRunner from "../service/TranslationRunner.js";

const runTest = async () => {
  try {
    const runner = new TranslationRunner({});
    const outDir = await runner.generateBulkTranslations();
    console.log(`✅ Translation job completed. Output at: ${outDir}`);
  } catch (err) {
    console.error(`❌ Translation job failed:`, err?.message || err);
    typeof Logger?.writeLog === "function"
      ? Logger.writeLog({
          flag: "tr_error",
          action: "fatal",
          message: err?.message || String(err),
          critical: true,
          data: { time: Date.now() },
        })
      : null;
    process.exitCode = 1;
  }
};
runTest();
