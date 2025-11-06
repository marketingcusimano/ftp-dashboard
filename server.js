app.get('/peek', async (_req, res) => {
  try {
    const { Client } = await import('basic-ftp');
    const {
      FTP_HOST, FTP_USER, FTP_PASS, FTP_FILE,
      FTP_SECURE, FTP_TIMEOUT, FTP_TLS_INSECURE, FTP_TLS_SERVERNAME
    } = process.env;
    const client = new Client(Number(FTP_TIMEOUT || 20000));
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: String(FTP_SECURE).toLowerCase() === 'true',
      secureOptions: {
        rejectUnauthorized: String(FTP_TLS_INSECURE).toLowerCase() === 'true' ? false : true,
        servername: FTP_TLS_SERVERNAME || FTP_HOST
      }
    });
    const tmp = '/tmp/testpeek_' + Date.now() + '.txt';
    await client.downloadTo(tmp, FTP_FILE);
    const txt = fs.readFileSync(tmp, 'utf8');
    client.close();
    try { fs.unlinkSync(tmp); } catch {}
    // Inviamo solo le prime 30 righe per vedere i separatori reali
    const preview = txt.split(/\r?\n/).slice(0, 30);
    res.type('text').send(preview.join('\n'));
  } catch (e) {
    res.status(500).send(String(e));
  }
});
