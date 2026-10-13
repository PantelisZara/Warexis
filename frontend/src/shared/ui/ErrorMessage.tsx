export function ErrorMessage({ message, title = 'Unable to complete the request' }: { message: string; title?: string }) {
  return <div className="message message--error" role="alert"><strong>{title}</strong><p>{message}</p></div>
}
