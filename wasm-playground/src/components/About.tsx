export function About() {
  return (
    <>
      <div className="">
        <header className="text-3xl text-center">
          <div className="mx-auto max-w-7xl py-6">
            <h1 className="text-3xl font-bold tracking-tight text-gray-900">DataFusion <br /> Playground</h1>
          </div>
        </header>
        <div className="explain">
          <p>
            Playground of
            {" "}
            <a href="https://github.com/datafusion-contrib/datafusion-wasm-playground">
              Apache DataFusion
            </a>
            .
          </p>
          <p className='c-gray'>It might take a while to load the wasm file.</p>
        </div>
      </div >
    </>
  )
}
