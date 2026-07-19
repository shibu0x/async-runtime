use crate::TaskList;

//a poll enum and here we are storing if the task will be ready or not
pub enum Poll {
    Ready,
    Pending,
}

//the struct future to store the value of per task
pub struct Task {
    pub task_name: String,
    pub rounds: u64,
    pub target: u64
}

//a trait to keep calling poll
pub trait MyFuture {
    fn poll(&mut self) -> Poll;
}

impl MyFuture for Task {
    fn poll(&mut self) -> Poll {
       if self.rounds < self.target {
            println!("{} is not completed, {} rounds done",self.task_name,self.rounds);
            self.rounds += 1;
            Poll::Pending
        } else{
            println!("{} is completed after {} rounds",self.task_name,self.rounds);
            Poll::Ready
        }
    }
}

//an event loop that drive all tasks concurrently until completion
pub fn run(mut task_list:TaskList) -> Result<(),()>{

    // keep cycling as long as there are active futures to poll
    while !task_list.tasks.is_empty() {

        // advance state and clean up finished tasks in a single pass
        task_list.tasks.retain_mut(|task| {
            let response = task.poll();  
            match response{
                Poll::Pending => true, //keep active task for the next iteration
                Poll::Ready => false   //remove completed task to avoid re-polling
            }
        });
    }
    Ok(())
}