use pyo3::prelude::*;
use pyo3_polars::PolarsAllocator;

mod expressions;

#[pymodule]
fn _polars_scheduler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();