// Minimal multipart/form-data builder for k6. It preserves repeated fields,
// which the plain object request form cannot represent.
export class FormData {
  constructor() {
    this.boundary = `----k6FormDataBoundary${Math.random().toString(36)}`;
    this.parts = [];
  }

  append(fieldName, data) {
    const file = typeof data === 'string' ? { data, content_type: 'text/plain' } : data;
    this.parts.push({ field: fieldName, file });
  }

  body() {
    const bytes = [];
    const appendText = value => {
      for (let i = 0; i < value.length; i += 1) bytes.push(value.charCodeAt(i) & 0xff);
    };
    for (const part of this.parts) {
      appendText(`--${this.boundary}\r\n`);
      let disposition = `Content-Disposition: form-data; name="${part.field}"`;
      if (part.file.filename) disposition += `; filename="${part.file.filename.replace(/"/g, '%22')}"`;
      appendText(`${disposition}\r\nContent-Type: ${part.file.content_type || 'application/octet-stream'}\r\n\r\n`);
      const data = Array.isArray(part.file.data) ? part.file.data : part.file.data;
      if (typeof data === 'string') appendText(data);
      else if (data && data.byteLength) {
        const view = data instanceof Uint8Array ? data : new Uint8Array(data);
        for (const value of view) bytes.push(value & 0xff);
      }
      appendText('\r\n');
    }
    appendText(`--${this.boundary}--\r\n`);
    return new Uint8Array(bytes).buffer;
  }
}
